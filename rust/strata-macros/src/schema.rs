//! `#[derive(StrataSchema)]` (`design/44-LLM-Integration.md`, "Structured
//! output"): the JSON Schema of a struct with named fields or of a fieldless
//! enum, as serde reads it.

use proc_macro2::TokenStream;
use quote::quote;
use syn::ext::IdentExt;
use syn::{
    Attribute, Data, DeriveInput, Expr, ExprLit, Fields, GenericArgument, Lit, Meta, PathArguments,
    Type,
};

pub fn derive(ast: &DeriveInput) -> syn::Result<TokenStream> {
    let container = SerdeAttrs::of(&ast.attrs)?;
    let body = match &ast.data {
        Data::Struct(data) => match &data.fields {
            Fields::Named(named) => object(named, &container)?,
            _ => return Err(unsupported(ast)),
        },
        Data::Enum(data) => {
            let mut names = Vec::new();
            for variant in &data.variants {
                if !matches!(variant.fields, Fields::Unit) {
                    return Err(unsupported(ast));
                }
                let attrs = SerdeAttrs::of(&variant.attrs)?;
                if attrs.skip {
                    continue;
                }
                names.push(attrs.rename.unwrap_or_else(|| {
                    container
                        .rename_all
                        .apply_to_variant(&variant.ident.unraw().to_string())
                }));
            }
            quote! {
                ::datalium_strata::serde_json::json!({ "type": "string", "enum": [#(#names),*] })
            }
        }
        Data::Union(_) => return Err(unsupported(ast)),
    };

    let ident = &ast.ident;
    // Each type parameter is described by its own schema, as serde's derive
    // bounds each by `Deserialize`.
    let mut generics = ast.generics.clone();
    for param in generics.type_params_mut() {
        param
            .bounds
            .push(syn::parse_quote!(::datalium_strata::tools::StrataSchema));
    }
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();
    Ok(quote! {
        impl #impl_generics ::datalium_strata::tools::StrataSchema for #ident #ty_generics #where_clause {
            fn json_schema() -> ::datalium_strata::serde_json::Value {
                #body
            }
        }
    })
}

fn unsupported(ast: &DeriveInput) -> syn::Error {
    syn::Error::new_spanned(
        &ast.ident,
        "#[derive(StrataSchema)] supports structs with named fields and enums whose variants have no fields",
    )
}

/// An `object` with a property per field (serde's name for it) and the
/// fields it can't do without as `required`.
fn object(named: &syn::FieldsNamed, container: &SerdeAttrs) -> syn::Result<TokenStream> {
    let mut properties = Vec::new();
    let mut required = Vec::new();
    for field in &named.named {
        let attrs = SerdeAttrs::of(&field.attrs)?;
        if attrs.skip {
            continue;
        }
        let ident = field
            .ident
            .as_ref()
            .expect("named field iteration always yields an ident");
        let name = attrs.rename.unwrap_or_else(|| {
            container
                .rename_all
                .apply_to_field(&ident.unraw().to_string())
        });
        let (schema, optional) = match option_inner(&field.ty) {
            Some(inner) => (schema_of(inner), true),
            None => (schema_of(&field.ty), false),
        };
        properties.push(quote! { properties.insert(#name.to_string(), #schema); });
        if !optional && !attrs.default {
            required.push(name);
        }
    }
    Ok(quote! {
        let mut properties = ::datalium_strata::serde_json::Map::new();
        #(#properties)*
        let required: ::std::vec::Vec<::datalium_strata::serde_json::Value> = ::std::vec![
            #( ::datalium_strata::serde_json::Value::String(#required.to_string()) ),*
        ];
        ::datalium_strata::serde_json::json!({
            "type": "object",
            "properties": ::datalium_strata::serde_json::Value::Object(properties),
            "required": required,
        })
    })
}

/// The schema of a field's type: primitives and the standard containers by
/// their spelling (so a `Vec` of a type nobody derived for is still an
/// `array`), anything else by its own `StrataSchema`, or `{"type": "object"}`
/// when it has none (`::datalium_strata::tools::__schema`).
fn schema_of(ty: &Type) -> TokenStream {
    let primitive = |json: &str| quote! { ::datalium_strata::serde_json::json!({ "type": #json }) };
    let array = |items: &Type| {
        let items = schema_of(items);
        quote! {{ let items = #items; ::datalium_strata::serde_json::json!({ "type": "array", "items": items }) }}
    };
    match ty {
        Type::Reference(reference) => return schema_of(&reference.elem),
        Type::Paren(paren) => return schema_of(&paren.elem),
        Type::Group(group) => return schema_of(&group.elem),
        Type::Slice(slice) => return array(&slice.elem),
        Type::Array(array_ty) => return array(&array_ty.elem),
        Type::Path(path) if path.qself.is_none() => {
            if let Some(segment) = path.path.segments.last() {
                let args = type_args(&segment.arguments);
                match (segment.ident.to_string().as_str(), args.as_slice()) {
                    ("String" | "str" | "char", []) => return primitive("string"),
                    ("bool", []) => return primitive("boolean"),
                    (
                        "i8" | "i16" | "i32" | "i64" | "i128" | "isize" | "u8" | "u16" | "u32"
                        | "u64" | "u128" | "usize",
                        [],
                    ) => return primitive("integer"),
                    ("f32" | "f64", []) => return primitive("number"),
                    ("Option" | "Box" | "Arc" | "Rc" | "Cow", [.., inner]) => {
                        return schema_of(inner);
                    }
                    ("Vec" | "VecDeque" | "HashSet" | "BTreeSet" | "LinkedList", [items, ..]) => {
                        return array(items);
                    }
                    ("HashMap" | "BTreeMap" | "IndexMap", [_, values, ..]) => {
                        let values = schema_of(values);
                        return quote! {{
                            let values = #values;
                            ::datalium_strata::serde_json::json!({
                                "type": "object",
                                "additionalProperties": values,
                            })
                        }};
                    }
                    _ => {}
                }
            }
        }
        _ => {}
    }
    quote! {{
        #[allow(unused_imports)]
        use ::datalium_strata::tools::__schema::{ViaFallback as _, ViaSchema as _};
        (&::datalium_strata::tools::__schema::Probe::<#ty>::new()).schema()
    }}
}

/// `T` when `ty` is spelled `Option<T>`.
fn option_inner(ty: &Type) -> Option<&Type> {
    let Type::Path(path) = ty else {
        return None;
    };
    let segment = path.path.segments.last()?;
    if segment.ident != "Option" {
        return None;
    }
    match type_args(&segment.arguments).as_slice() {
        [inner] => Some(inner),
        _ => None,
    }
}

fn type_args(arguments: &PathArguments) -> Vec<&Type> {
    match arguments {
        PathArguments::AngleBracketed(args) => args
            .args
            .iter()
            .filter_map(|arg| match arg {
                GenericArgument::Type(ty) => Some(ty),
                _ => None,
            })
            .collect(),
        _ => Vec::new(),
    }
}

/// The `#[serde(...)]` keys the schema depends on. Everything else is
/// serde's business and is ignored here.
#[derive(Default)]
struct SerdeAttrs {
    rename: Option<String>,
    rename_all: RenameRule,
    skip: bool,
    default: bool,
}

impl SerdeAttrs {
    fn of(attrs: &[Attribute]) -> syn::Result<Self> {
        let mut out = Self::default();
        for attr in attrs.iter().filter(|attr| attr.path().is_ident("serde")) {
            attr.parse_nested_meta(|meta| {
                if meta.path.is_ident("rename") {
                    if let Some(name) = deserialize_name(&meta)? {
                        out.rename = Some(name);
                    }
                } else if meta.path.is_ident("rename_all") {
                    if let Some(rule) = deserialize_name(&meta)? {
                        out.rename_all = RenameRule::parse(&rule)
                            .ok_or_else(|| meta.error(format!("unknown rename rule `{rule}`")))?;
                    }
                } else if meta.path.is_ident("skip") || meta.path.is_ident("skip_deserializing") {
                    out.skip = true;
                } else if meta.path.is_ident("default") {
                    out.default = true;
                }
                // Any other key, with whatever value it has.
                if meta.input.peek(syn::Token![=]) {
                    meta.value()?.parse::<Expr>()?;
                } else if meta.input.peek(syn::token::Paren) {
                    meta.parse_nested_meta(|nested| {
                        if nested.input.peek(syn::Token![=]) {
                            nested.value()?.parse::<Expr>()?;
                        }
                        Ok(())
                    })?;
                }
                Ok(())
            })?;
        }
        Ok(out)
    }
}

/// The name a `rename` / `rename_all` gives deserialization: `= "x"`, or
/// `(deserialize = "x")` (`serialize` alone leaves it as it is). The value
/// is left in the input for [`SerdeAttrs::of`] to skip when it isn't
/// consumed here.
fn deserialize_name(meta: &syn::meta::ParseNestedMeta) -> syn::Result<Option<String>> {
    if meta.input.peek(syn::Token![=]) {
        let fork = meta.input.fork();
        fork.parse::<syn::Token![=]>()?;
        return Ok(Some(string(&fork.parse::<Expr>()?)?));
    }
    let mut name = None;
    if meta.input.peek(syn::token::Paren) {
        let fork = meta.input.fork();
        let content;
        syn::parenthesized!(content in fork);
        let pairs = content.parse_terminated(<Meta as syn::parse::Parse>::parse, syn::Token![,])?;
        for pair in pairs {
            if let Meta::NameValue(pair) = pair
                && pair.path.is_ident("deserialize")
            {
                name = Some(string(&pair.value)?);
            }
        }
    }
    Ok(name)
}

fn string(expr: &Expr) -> syn::Result<String> {
    match expr {
        Expr::Lit(ExprLit {
            lit: Lit::Str(s), ..
        }) => Ok(s.value()),
        _ => Err(syn::Error::new_spanned(expr, "expected a string literal")),
    }
}

/// serde's `rename_all` rules, applied as serde applies them: to a field's
/// `snake_case` name, or to a variant's `PascalCase` one.
#[derive(Default, Clone, Copy)]
enum RenameRule {
    #[default]
    None,
    Lower,
    Upper,
    Pascal,
    Camel,
    Snake,
    ScreamingSnake,
    Kebab,
    ScreamingKebab,
}

impl RenameRule {
    fn parse(rule: &str) -> Option<Self> {
        Some(match rule {
            "lowercase" => Self::Lower,
            "UPPERCASE" => Self::Upper,
            "PascalCase" => Self::Pascal,
            "camelCase" => Self::Camel,
            "snake_case" => Self::Snake,
            "SCREAMING_SNAKE_CASE" => Self::ScreamingSnake,
            "kebab-case" => Self::Kebab,
            "SCREAMING-KEBAB-CASE" => Self::ScreamingKebab,
            _ => return None,
        })
    }

    fn apply_to_variant(self, variant: &str) -> String {
        let snake = || {
            let mut out = String::new();
            for (i, ch) in variant.char_indices() {
                if i > 0 && ch.is_uppercase() {
                    out.push('_');
                }
                out.push(ch.to_ascii_lowercase());
            }
            out
        };
        match self {
            Self::None | Self::Pascal => variant.to_string(),
            Self::Lower => variant.to_ascii_lowercase(),
            Self::Upper => variant.to_ascii_uppercase(),
            Self::Camel => {
                let mut chars = variant.chars();
                chars.next().map_or_else(String::new, |first| {
                    first.to_ascii_lowercase().to_string() + chars.as_str()
                })
            }
            Self::Snake => snake(),
            Self::ScreamingSnake => snake().to_ascii_uppercase(),
            Self::Kebab => snake().replace('_', "-"),
            Self::ScreamingKebab => snake().to_ascii_uppercase().replace('_', "-"),
        }
    }

    fn apply_to_field(self, field: &str) -> String {
        let pascal = || {
            let mut out = String::new();
            let mut capitalize = true;
            for ch in field.chars() {
                if ch == '_' {
                    capitalize = true;
                } else if capitalize {
                    out.push(ch.to_ascii_uppercase());
                    capitalize = false;
                } else {
                    out.push(ch);
                }
            }
            out
        };
        match self {
            Self::None | Self::Lower | Self::Snake => field.to_string(),
            Self::Upper | Self::ScreamingSnake => field.to_ascii_uppercase(),
            Self::Pascal => pascal(),
            Self::Camel => {
                let pascal = pascal();
                let mut chars = pascal.chars();
                chars.next().map_or_else(String::new, |first| {
                    first.to_ascii_lowercase().to_string() + chars.as_str()
                })
            }
            Self::Kebab => field.replace('_', "-"),
            Self::ScreamingKebab => field.to_ascii_uppercase().replace('_', "-"),
        }
    }
}
