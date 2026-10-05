//! Which real-time options each worker thread and socket accepts, and
//! applying them.

use std::io;

use fast_talker::options::{Policy, Report, Rules, SocketOption, ThreadOption};
use fast_talker::rt::{Scheduler, ThreadPriority};

use crate::error::CameraError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ThreadRole {
    /// The GVCP worker: request/response, heartbeat, events.
    Control,
    /// The GVSP worker: reassembly and fan-out at line rate.
    Stream,
}

impl ThreadRole {
    fn driver(self) -> &'static str {
        match self {
            Self::Control => "gvcp",
            Self::Stream => "gvsp",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SocketRole {
    /// The GVCP socket: commands, acknowledges and device events.
    UdpControl,
    /// The GVSP socket: receive-only image data.
    UdpStreamRx,
}

impl SocketRole {
    fn driver(self) -> &'static str {
        match self {
            Self::UdpControl => "gvcp",
            Self::UdpStreamRx => "gvsp",
        }
    }
}

pub(crate) fn thread_allowed(role: ThreadRole, o: &ThreadOption) -> bool {
    match role {
        ThreadRole::Stream => match o {
            ThreadOption::CpuAffinity(_)
            | ThreadOption::RtPriority(_)
            | ThreadOption::PrefaultStack(_)
            | ThreadOption::UnixScheduler(_)
            | ThreadOption::LinuxNice(_)
            | ThreadOption::WinPriority(_)
            | ThreadOption::WinDisablePowerThrottling
            | ThreadOption::WinMmcss(_)
            | ThreadOption::MacOsQos(_) => true,
            ThreadOption::MacOsTimeConstraint { .. } => false,
            _ => false,
        },
        ThreadRole::Control => match o {
            ThreadOption::CpuAffinity(_)
            | ThreadOption::PrefaultStack(_)
            | ThreadOption::LinuxNice(_)
            | ThreadOption::WinDisablePowerThrottling
            | ThreadOption::MacOsQos(_) => true,
            ThreadOption::UnixScheduler(s) => {
                matches!(s, Scheduler::Other | Scheduler::Batch | Scheduler::Idle)
            }
            ThreadOption::WinPriority(p) => *p != ThreadPriority::TimeCritical,
            _ => false,
        },
    }
}

pub(crate) fn socket_allowed(role: SocketRole, o: &SocketOption) -> bool {
    match role {
        SocketRole::UdpStreamRx => matches!(
            o,
            SocketOption::RecvBuffer(_)
                | SocketOption::BindDevice(_)
                | SocketOption::LinuxBusyPoll(_)
                | SocketOption::LinuxPreferBusyPoll(_)
                | SocketOption::LinuxBusyPollBudget(_)
                | SocketOption::WinCpuAffinity(_)
        ),
        SocketRole::UdpControl => matches!(
            o,
            SocketOption::RecvBuffer(_)
                | SocketOption::BindDevice(_)
                | SocketOption::Dscp(_)
                | SocketOption::LinuxPriority(_)
        ),
    }
}

pub(crate) fn check_thread(role: ThreadRole, options: &[ThreadOption]) -> Result<(), CameraError> {
    match options.iter().find(|o| !thread_allowed(role, o)) {
        Some(o) => Err(CameraError::InvalidOption {
            option: format!("{o:?}"),
            driver: role.driver(),
        }),
        None => Ok(()),
    }
}

pub(crate) fn check_socket(role: SocketRole, options: &[SocketOption]) -> Result<(), CameraError> {
    match options.iter().find(|o| !socket_allowed(role, o)) {
        Some(o) => Err(CameraError::InvalidOption {
            option: format!("{o:?}"),
            driver: role.driver(),
        }),
        None => Ok(()),
    }
}

/// Applies `options` to the calling thread. Keep the returned report alive
/// for as long as the thread runs: some options are released when it drops.
pub(crate) fn apply_thread(
    role: ThreadRole,
    options: &[ThreadOption],
) -> io::Result<Report<ThreadOption>> {
    let allow = |o: &ThreadOption| thread_allowed(role, o);
    let rules = Rules {
        other_platform: Policy::Report,
        unsupported: Policy::Report,
        rejected: Policy::Error,
        allow: Some(&allow),
    };
    let report = ThreadOption::apply_all(options, &rules)?;
    for s in &report.skipped {
        tracing::warn!(
            option = ?s.option,
            reason = %s.reason,
            "{} thread option skipped",
            role.driver()
        );
    }
    Ok(report)
}

#[cfg(unix)]
pub(crate) fn apply_socket(
    role: SocketRole,
    socket: &impl std::os::fd::AsFd,
    options: &[SocketOption],
) -> io::Result<Report<SocketOption>> {
    let allow = |o: &SocketOption| socket_allowed(role, o);
    let report = SocketOption::apply_all(socket, options, &socket_rules(&allow))?;
    log_skipped_sockets(role, &report);
    Ok(report)
}

#[cfg(windows)]
pub(crate) fn apply_socket(
    role: SocketRole,
    socket: &impl std::os::windows::io::AsSocket,
    options: &[SocketOption],
) -> io::Result<Report<SocketOption>> {
    let allow = |o: &SocketOption| socket_allowed(role, o);
    let report = SocketOption::apply_all(socket, options, &socket_rules(&allow))?;
    log_skipped_sockets(role, &report);
    Ok(report)
}

fn socket_rules<'a>(allow: &'a dyn Fn(&SocketOption) -> bool) -> Rules<'a, SocketOption> {
    Rules {
        other_platform: Policy::Report,
        unsupported: Policy::Report,
        rejected: Policy::Error,
        allow: Some(allow),
    }
}

fn log_skipped_sockets(role: SocketRole, report: &Report<SocketOption>) {
    for s in &report.skipped {
        tracing::warn!(
            option = ?s.option,
            reason = %s.reason,
            "{} socket option skipped",
            role.driver()
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn control_thread_refuses_real_time_classes() {
        let refused = [
            ThreadOption::RtPriority(80),
            ThreadOption::UnixScheduler(Scheduler::Fifo(10)),
            ThreadOption::UnixScheduler(Scheduler::RoundRobin(10)),
            ThreadOption::WinPriority(ThreadPriority::TimeCritical),
            ThreadOption::WinMmcss("Pro Audio".into()),
            ThreadOption::MacOsTimeConstraint {
                period_us: 1000,
                computation_us: 100,
                constraint_us: 500,
            },
        ];
        for o in &refused {
            assert!(!thread_allowed(ThreadRole::Control, o), "{o:?}");
        }
        assert!(thread_allowed(
            ThreadRole::Control,
            &ThreadOption::UnixScheduler(Scheduler::Batch)
        ));
        assert!(thread_allowed(
            ThreadRole::Control,
            &ThreadOption::WinPriority(ThreadPriority::Highest)
        ));
    }

    #[test]
    fn stream_thread_refuses_only_time_constraint() {
        assert!(thread_allowed(
            ThreadRole::Stream,
            &ThreadOption::RtPriority(80)
        ));
        assert!(!thread_allowed(
            ThreadRole::Stream,
            &ThreadOption::MacOsTimeConstraint {
                period_us: 1000,
                computation_us: 100,
                constraint_us: 500,
            }
        ));
    }

    #[test]
    fn socket_lists() {
        assert!(socket_allowed(
            SocketRole::UdpStreamRx,
            &SocketOption::RecvBuffer(1 << 20)
        ));
        assert!(!socket_allowed(
            SocketRole::UdpStreamRx,
            &SocketOption::Dscp(46)
        ));
        assert!(socket_allowed(
            SocketRole::UdpControl,
            &SocketOption::Dscp(46)
        ));
        assert!(!socket_allowed(
            SocketRole::UdpControl,
            &SocketOption::LinuxBusyPoll(50)
        ));
        let err = check_socket(SocketRole::UdpStreamRx, &[SocketOption::SendBuffer(1)]);
        assert!(matches!(
            err,
            Err(CameraError::InvalidOption { driver: "gvsp", .. })
        ));
    }
}
