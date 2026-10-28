//! Procedural macros backing `datalium-strata`'s tool-calling metaprogramming
//! (STRATA-SPEC-003). Both macros emit code that references the host crate
//! exclusively via the fully-qualified `::datalium_strata::...` path (including
//! `::datalium_strata::serde_json`, a re-export), so callers never need their
//! own direct `serde_json` dependency and macro hygiene doesn't depend on
//! whatever the call site happens to have `use`d.
//!
//! No runtime reflection or type downcasting is used by either macro's
//! generated output — dispatch is a static match over string tool names
//! calling directly-named functions.

use proc_macro::TokenStream;
use proc_macro2::Span;
use quote::quote;
use syn::parse::Parser;
use syn::punctuated::Punctuated;
use syn::{
    DeriveInput, Expr, ExprLit, FnArg, ItemFn, Lit, MetaNameValue, Token, Type, parse_macro_input,
};

mod schema;

/// `#[derive(StrataSchema)]` — implements `StrataSchema::json_schema()` for a
/// struct with named fields (an `object`) or an enum whose variants have no
/// fields (a `string` with an `enum` of the variants' names), as serde reads
/// the type (`design/44-LLM-Integration.md`, "Structured output").
///
/// - A field's type is described by its spelling: strings, booleans and
///   numbers as JSON-Schema primitives; `Vec<T>`, slices, arrays and sets as
///   an `array` with `items`; `HashMap<String, T>` / `BTreeMap<String, T>` as
///   an `object` with `additionalProperties`; `Box<T>`, `Arc<T>` as `T`. Any
///   other type is described by its own `StrataSchema` (a nested derived
///   type), or as `{"type": "object"}` when it has none.
/// - `Option<T>` fields are described as `T` and left out of `required`, as
///   are `#[serde(default)]` ones (STRATA-SPEC-003 section 5's optional-field
///   rule); `#[serde(skip)]` / `skip_deserializing` ones aren't described.
/// - `#[serde(rename = "…")]` and `#[serde(rename_all = "…")]` (or their
///   `deserialize = "…"` form) name the properties and variants as serde does.
#[proc_macro_derive(StrataSchema, attributes(serde))]
pub fn derive_strata_schema(input: TokenStream) -> TokenStream {
    let ast = parse_macro_input!(input as DeriveInput);
    schema::derive(&ast)
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}

/// `#[strata_tool(name = "...", description = "...")]` — applied to a free
/// `async fn(args: ArgsType) -> Result<String, E>` taking one argument
/// struct, optionally followed by a `ctx: ToolCallContext` that receives the
/// call's context (`design/20-Tool-Dispatch-And-Registry.md`) and is not part
/// of the manifest. Preserves the original function and additionally emits a
/// zero-sized `<PascalCase(fn_name)>Tool` struct implementing
/// `::datalium_strata::tools::StrataTool`, whose `schema()` requires
/// `ArgsType: StrataSchema` (a missing `#[derive(StrataSchema)]` on the
/// argument struct is therefore a compile-time trait-bound failure, not a
/// silent runtime schema/argument mismatch) and whose `execute()` deserializes
/// raw JSON arguments and calls the original function directly — no
/// reflection, just a static call.
#[proc_macro_attribute]
pub fn strata_tool(attr: TokenStream, item: TokenStream) -> TokenStream {
    let parser = Punctuated::<MetaNameValue, Token![,]>::parse_terminated;
    let args = match parser.parse(attr) {
        Ok(a) => a,
        Err(e) => return e.to_compile_error().into(),
    };

    let mut tool_name: Option<String> = None;
    let mut tool_description: Option<String> = None;
    for nv in &args {
        let value_str = match &nv.value {
            Expr::Lit(ExprLit {
                lit: Lit::Str(s), ..
            }) => s.value(),
            _ => {
                return syn::Error::new_spanned(&nv.value, "expected a string literal")
                    .to_compile_error()
                    .into();
            }
        };
        if nv.path.is_ident("name") {
            tool_name = Some(value_str);
        } else if nv.path.is_ident("description") {
            tool_description = Some(value_str);
        } else {
            return syn::Error::new_spanned(
                &nv.path,
                "unknown #[strata_tool] argument, expected `name` or `description`",
            )
            .to_compile_error()
            .into();
        }
    }

    let (Some(tool_name), Some(tool_description)) = (tool_name, tool_description) else {
        return syn::Error::new(
            Span::call_site(),
            "#[strata_tool] requires both `name` and `description`, e.g. \
             #[strata_tool(name = \"...\", description = \"...\")]",
        )
        .to_compile_error()
        .into();
    };

    let input_fn = parse_macro_input!(item as ItemFn);

    if input_fn.sig.asyncness.is_none() {
        return syn::Error::new_spanned(
            &input_fn.sig,
            "#[strata_tool] functions must be declared `async fn`",
        )
        .to_compile_error()
        .into();
    }

    let inputs = &input_fn.sig.inputs;
    let wrong_parameters = || {
        syn::Error::new_spanned(
            inputs,
            "#[strata_tool] functions take an arguments struct implementing StrataSchema + \
             Deserialize, optionally followed by a `ToolCallContext`",
        )
        .to_compile_error()
        .into()
    };
    if inputs.is_empty() || inputs.len() > 2 {
        return wrong_parameters();
    }

    let mut param_types = Vec::new();
    for input in inputs {
        match input {
            FnArg::Typed(pat_type) => param_types.push((*pat_type.ty).clone()),
            FnArg::Receiver(receiver) => {
                return syn::Error::new_spanned(
                    receiver,
                    "#[strata_tool] functions must be free functions, not methods",
                )
                .to_compile_error()
                .into();
            }
        }
    }
    // The context is recognized by name here; a different type of that name
    // fails to compile at the call below, with an ordinary type mismatch.
    if is_tool_call_context(&param_types[0]) {
        return wrong_parameters();
    }
    let takes_context = match param_types.get(1) {
        Some(ty) if is_tool_call_context(ty) => true,
        Some(_) => return wrong_parameters(),
        None => false,
    };
    let arg_type = param_types[0].clone();

    let fn_ident = &input_fn.sig.ident;
    let tool_struct_ident = to_pascal_case_tool_ident(fn_ident);
    let call = if takes_context {
        quote! { #fn_ident(parsed_args, ctx) }
    } else {
        quote! { { drop(ctx); #fn_ident(parsed_args) } }
    };

    let expanded = quote! {
        #input_fn

        #[allow(non_camel_case_types)]
        #[doc = "Zero-sized dispatch handle generated by `#[strata_tool]`."]
        pub struct #tool_struct_ident;

        impl ::datalium_strata::tools::StrataTool for #tool_struct_ident {
            fn name(&self) -> &'static str {
                #tool_name
            }

            fn schema(&self) -> ::datalium_strata::serde_json::Value {
                ::datalium_strata::serde_json::json!({
                    "name": #tool_name,
                    "description": #tool_description,
                    "parameters": <#arg_type as ::datalium_strata::tools::StrataSchema>::json_schema(),
                })
            }

            fn execute<'a>(
                &'a self,
                raw_args: &'a str,
            ) -> ::std::pin::Pin<
                ::std::boxed::Box<
                    dyn ::std::future::Future<Output = Result<String, ::datalium_strata::StrataError>>
                        + Send
                        + 'a,
                >,
            > {
                ::datalium_strata::tools::StrataTool::execute_with(
                    self,
                    raw_args,
                    ::datalium_strata::tools::ToolCallContext::default(),
                )
            }

            fn execute_with<'a>(
                &'a self,
                raw_args: &'a str,
                ctx: ::datalium_strata::tools::ToolCallContext,
            ) -> ::std::pin::Pin<
                ::std::boxed::Box<
                    dyn ::std::future::Future<Output = Result<String, ::datalium_strata::StrataError>>
                        + Send
                        + 'a,
                >,
            > {
                Box::pin(async move {
                    // Arguments that don't fit are the caller's mistake: said
                    // in the words both engines use, or serde's when those
                    // explain nothing (design/20).
                    let parsed_args: #arg_type = ::datalium_strata::serde_json::from_str(raw_args)
                        .map_err(|e| {
                            ::datalium_strata::StrataError::ToolArgumentParseFailure(
                                ::datalium_strata::tools::argument_problems(
                                    raw_args,
                                    &<#arg_type as ::datalium_strata::tools::StrataSchema>::json_schema(),
                                )
                                .unwrap_or_else(|| e.to_string()),
                            )
                        })?;
                    let output = #call.await.map_err(|_| {
                        ::datalium_strata::StrataError::ToolExecutionFailure(#tool_name.to_string())
                    })?;
                    Ok(output)
                })
            }
        }
    };

    expanded.into()
}

/// Whether `ty` is spelled `ToolCallContext` (with any path in front).
fn is_tool_call_context(ty: &Type) -> bool {
    matches!(ty, Type::Path(path)
        if path.path.segments.last().is_some_and(|s| s.ident == "ToolCallContext"))
}

/// `check_inventory` -> `CheckInventoryTool`.
fn to_pascal_case_tool_ident(fn_name: &syn::Ident) -> syn::Ident {
    let mut out = String::new();
    for part in fn_name.to_string().split('_') {
        let mut chars = part.chars();
        if let Some(first) = chars.next() {
            out.extend(first.to_uppercase());
            out.push_str(chars.as_str());
        }
    }
    out.push_str("Tool");
    syn::Ident::new(&out, fn_name.span())
}
