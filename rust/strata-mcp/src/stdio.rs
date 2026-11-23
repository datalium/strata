//! A server started as a child process, spoken to over its stdin and stdout
//! (`design/50-MCP-Client.md`, "stdio"): how it starts, where its stderr
//! goes, and how it is stopped with every process it started.

use std::ffi::{OsStr, OsString};
use std::path::PathBuf;
use std::process::Stdio;
use std::time::Duration;

use tokio::io::BufReader;
use tokio::process::{Child, Command};

use crate::McpError;
use crate::link::{LineError, PipeLink, Reader, read_line};
use crate::options::{Log, McpClientOptions};

/// A server Strata starts as a child process. Built with
/// [`McpServer::stdio`](crate::McpServer::stdio).
#[derive(Debug, Clone)]
pub struct StdioServer {
    command: OsString,
    args: Vec<OsString>,
    env: Vec<(OsString, Option<OsString>)>,
    inherit_env: bool,
    current_dir: Option<PathBuf>,
    shutdown_timeout: Duration,
}

impl StdioServer {
    pub(crate) fn new(command: OsString) -> Self {
        Self {
            command,
            args: Vec::new(),
            env: Vec::new(),
            inherit_env: true,
            current_dir: None,
            shutdown_timeout: Duration::from_secs(5),
        }
    }

    pub fn arg(mut self, arg: impl Into<OsString>) -> Self {
        self.args.push(arg.into());
        self
    }

    pub fn args<I, A>(mut self, args: I) -> Self
    where
        I: IntoIterator<Item = A>,
        A: Into<OsString>,
    {
        self.args.extend(args.into_iter().map(Into::into));
        self
    }

    /// Sets a variable in the server's environment.
    pub fn env(mut self, key: impl Into<OsString>, value: impl Into<OsString>) -> Self {
        self.env.push((key.into(), Some(value.into())));
        self
    }

    /// Removes a variable from the server's environment.
    pub fn env_remove(mut self, key: impl Into<OsString>) -> Self {
        self.env.push((key.into(), None));
        self
    }

    /// Whether the server inherits Strata's environment (the default). With
    /// `false` it gets only the variables given, plus `PATH` (and on
    /// Windows `SystemRoot`), which starting a process needs.
    pub fn inherit_env(mut self, inherit: bool) -> Self {
        self.inherit_env = inherit;
        self
    }

    pub fn current_dir(mut self, dir: impl Into<PathBuf>) -> Self {
        self.current_dir = Some(dir.into());
        self
    }

    /// How long closing waits for the server to exit once its stdin is
    /// closed, before it and its processes are stopped. 5 s by default.
    pub fn shutdown_timeout(mut self, timeout: Duration) -> Self {
        self.shutdown_timeout = timeout;
        self
    }

    /// The server's `PATH`: what its environment sets, else Strata's.
    fn path(&self) -> Option<OsString> {
        let set = self
            .env
            .iter()
            .rev()
            .find(|(key, _)| key.eq_ignore_ascii_case("PATH"))
            .map(|(_, value)| value.clone());
        match set {
            Some(value) => value,
            None => std::env::var_os("PATH"),
        }
    }
}

/// The running server process.
pub(crate) struct StdioProcess {
    child: tokio::sync::Mutex<Option<Child>>,
    pid: Option<u32>,
    shutdown_timeout: Duration,
}

/// Starts `server`: its process, the link over its stdin and stdout, and
/// the task draining its stderr.
pub(crate) fn spawn(
    server: &StdioServer,
    options: &McpClientOptions,
    log: &Log,
) -> Result<(PipeLink, StdioProcess), McpError> {
    let program = resolve(&server.command, server.path());
    let mut command = Command::new(&program);
    command
        .args(&server.args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    if let Some(dir) = &server.current_dir {
        command.current_dir(dir);
    }
    if !server.inherit_env {
        command.env_clear();
        for key in ["PATH", "SystemRoot"] {
            if let Some(value) = std::env::var_os(key) {
                command.env(key, value);
            }
        }
    }
    for (key, value) in &server.env {
        match value {
            Some(value) => command.env(key, value),
            None => command.env_remove(key),
        };
    }
    #[cfg(unix)]
    command.process_group(0);
    #[cfg(windows)]
    command.creation_flags(CREATE_NO_WINDOW);

    let mut child = command.spawn().map_err(|error| {
        McpError::Connect(format!(
            "couldn't start {}: {error}",
            program.to_string_lossy()
        ))
    })?;
    let stdin = child.stdin.take().expect("stdin is piped");
    let stdout = child.stdout.take().expect("stdout is piped");
    let stderr = child.stderr.take().expect("stderr is piped");
    #[cfg(windows)]
    let (stdout, stderr) = {
        let handle = |pipe: std::io::Result<std::os::windows::io::OwnedHandle>| {
            pipe.map(detached)
                .map_err(|error| McpError::Connect(format!("couldn't read the server: {error}")))
        };
        (
            handle(stdout.into_owned_handle())?,
            handle(stderr.into_owned_handle())?,
        )
    };
    #[cfg(not(windows))]
    let (stdout, stderr): (Reader, Reader) = (Box::new(stdout), Box::new(stderr));
    let stderr_log = log.clone();
    tokio::spawn(async move {
        let mut stderr = BufReader::new(stderr);
        loop {
            match read_line(&mut stderr, 64 * 1024).await {
                Ok(Some(line)) => {
                    stderr_log.debug(|| format!("stderr: {}", String::from_utf8_lossy(&line)))
                }
                // A line too long to keep is dropped; the rest is read on.
                Err(LineError::TooBig) => {}
                Ok(None) | Err(LineError::Io(_)) => break,
            }
        }
    });
    let pid = child.id();
    let link = PipeLink::start(
        stdout,
        Box::new(stdin),
        options.max_message_bytes,
        log.clone(),
        "the server exited",
    );
    Ok((
        link,
        StdioProcess {
            child: tokio::sync::Mutex::new(Some(child)),
            pid,
            shutdown_timeout: server.shutdown_timeout,
        },
    ))
}

#[cfg(windows)]
const CREATE_NO_WINDOW: u32 = 0x0800_0000;

/// On Windows tokio reads a child's pipes on its blocking pool, and a
/// runtime waits for those reads when it shuts down: a process that escaped
/// being stopped and keeps a pipe open would stall the application's
/// shutdown. A thread of its own reads instead, which nothing waits for; it
/// ends when the pipe does or nobody reads any more.
#[cfg(windows)]
fn detached(handle: std::os::windows::io::OwnedHandle) -> Reader {
    use std::io::Read;
    use tokio::io::AsyncWriteExt;

    let (reader, mut writer) = tokio::io::duplex(64 * 1024);
    let runtime = tokio::runtime::Handle::current();
    let mut pipe = std::fs::File::from(handle);
    // A thread that won't start drops the writer: the server reads as exited.
    let _ = std::thread::Builder::new()
        .name("strata-mcp-pipe".into())
        .spawn(move || {
            let mut buffer = vec![0u8; 64 * 1024];
            loop {
                let read = match pipe.read(&mut buffer) {
                    Ok(0) | Err(_) => break,
                    Ok(read) => read,
                };
                if runtime.block_on(writer.write_all(&buffer[..read])).is_err() {
                    break;
                }
            }
        });
    Box::new(reader)
}

impl StdioProcess {
    /// After its stdin was closed: waits for the server to exit, then stops
    /// whatever of its process tree is left.
    pub(crate) async fn shutdown(&self) {
        let Some(mut child) = self.child.lock().await.take() else {
            return;
        };
        if tokio::time::timeout(self.shutdown_timeout, child.wait())
            .await
            .is_ok()
        {
            // The server exited; processes it started may not have.
            #[cfg(unix)]
            if let Some(pid) = self.pid {
                signal_group(pid, libc::SIGKILL);
            }
            return;
        }
        self.force(child).await;
    }

    #[cfg(unix)]
    async fn force(&self, mut child: Child) {
        if let Some(pid) = self.pid {
            signal_group(pid, libc::SIGTERM);
            let _ = tokio::time::timeout(Duration::from_secs(1), child.wait()).await;
            signal_group(pid, libc::SIGKILL);
        }
        let _ = child.kill().await;
    }

    #[cfg(windows)]
    async fn force(&self, mut child: Child) {
        if let Some(pid) = self.pid {
            let _ = Command::new(taskkill())
                .args(["/T", "/F", "/PID", &pid.to_string()])
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .creation_flags(CREATE_NO_WINDOW)
                .status()
                .await;
        }
        let _ = child.kill().await;
    }

    #[cfg(not(any(unix, windows)))]
    async fn force(&self, mut child: Child) {
        let _ = child.kill().await;
    }
}

impl Drop for StdioProcess {
    /// Closing wasn't awaited: the process tree is stopped at once.
    fn drop(&mut self) {
        let Some(mut child) = self.child.get_mut().take() else {
            return;
        };
        if let Some(pid) = self.pid {
            #[cfg(unix)]
            signal_group(pid, libc::SIGKILL);
            // taskkill walks the tree from the server, so the server must
            // still be running when it does: on a thread of its own, since
            // drop can't wait, and the server (kill_on_drop) after it.
            #[cfg(windows)]
            {
                use std::os::windows::process::CommandExt;
                // A thread that won't start drops the closure, and the server
                // with it.
                let _ = std::thread::Builder::new()
                    .name("strata-mcp-stop".into())
                    .spawn(move || {
                        let _ = std::process::Command::new(taskkill())
                            .args(["/T", "/F", "/PID", &pid.to_string()])
                            .stdout(Stdio::null())
                            .stderr(Stdio::null())
                            .creation_flags(CREATE_NO_WINDOW)
                            .status();
                        drop(child);
                    });
                return;
            }
            #[cfg(not(any(unix, windows)))]
            let _ = pid;
        }
        let _ = child.start_kill();
    }
}

/// Signals the server's process group, which it leads: it was started in a
/// group of its own.
#[cfg(unix)]
fn signal_group(pid: u32, signal: libc::c_int) {
    if let Ok(pid) = libc::pid_t::try_from(pid) {
        // SAFETY: kill() takes plain integers; a group that is gone is ESRCH.
        unsafe {
            libc::kill(-pid, signal);
        }
    }
}

#[cfg(windows)]
fn taskkill() -> PathBuf {
    std::env::var_os("SystemRoot")
        .map(|root| PathBuf::from(root).join("System32").join("taskkill.exe"))
        .unwrap_or_else(|| PathBuf::from("taskkill.exe"))
}

/// The program to start. On Windows a bare command is looked up in `PATH`
/// with each `PATHEXT` extension, as a shell would: `Command` looks for
/// `.exe` only, and `npx` is `npx.cmd`. A `.cmd` / `.bat` found that way is
/// run by the standard library through `cmd.exe`, which quotes its
/// arguments for batch files (and refuses ones it can't quote safely).
#[cfg(windows)]
fn resolve(command: &OsStr, path: Option<OsString>) -> OsString {
    let as_path = std::path::Path::new(command);
    if as_path.extension().is_some() {
        return command.to_os_string();
    }
    let extensions = std::env::var_os("PATHEXT")
        .map(|value| value.to_string_lossy().into_owned())
        .unwrap_or_else(|| ".COM;.EXE;.BAT;.CMD".to_string());
    let with_extension = |base: &std::path::Path| {
        extensions
            .split(';')
            .filter(|e| !e.is_empty())
            .map(|extension| {
                let mut name = base.as_os_str().to_os_string();
                name.push(extension);
                PathBuf::from(name)
            })
            .find(|candidate| candidate.is_file())
    };
    // A path (`tools\server`) tries the extensions on itself, not on PATH.
    if as_path.components().count() > 1 {
        return with_extension(as_path)
            .map_or_else(|| command.to_os_string(), PathBuf::into_os_string);
    }
    path.iter()
        .flat_map(std::env::split_paths)
        .find_map(|dir| with_extension(&dir.join(command)))
        .map_or_else(|| command.to_os_string(), PathBuf::into_os_string)
}

#[cfg(not(windows))]
fn resolve(command: &OsStr, _path: Option<OsString>) -> OsString {
    command.to_os_string()
}

#[cfg(all(test, windows))]
mod tests {
    use super::*;

    #[test]
    fn a_bare_command_is_found_through_pathext() {
        let dir = std::env::temp_dir().join(format!("strata-mcp-pathext-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("strata-fixture-tool.cmd"), "@echo off\r\n").unwrap();
        let found = resolve(
            OsStr::new("strata-fixture-tool"),
            Some(dir.clone().into_os_string()),
        );
        // PATHEXT's own case: `.CMD`.
        assert!(
            found
                .to_string_lossy()
                .eq_ignore_ascii_case(&dir.join("strata-fixture-tool.cmd").to_string_lossy())
        );
        let by_path = resolve(dir.join("strata-fixture-tool").as_os_str(), None);
        assert!(
            by_path
                .to_string_lossy()
                .eq_ignore_ascii_case(&dir.join("strata-fixture-tool.cmd").to_string_lossy())
        );
        let explicit = resolve(OsStr::new("node.exe"), Some(dir.clone().into_os_string()));
        assert_eq!(explicit, OsString::from("node.exe"));
        std::fs::remove_dir_all(dir).unwrap();
    }
}
