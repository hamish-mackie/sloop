//! Lease expiry as a truthful fact. The daemon renews the leases of runs it
//! supervises, re-arms a lapsed lease when recovery adopts a live run, and
//! never resurrects the lease of a run that died.

mod support;

use std::time::Duration;

use support::{FakeAgent, World, process_alive, wait_until};

/// The TTL the daemon takes at claim time and extends by on each renewal.
const LEASE_MS: i64 = 10 * 60 * 1000;

fn post_and_run(world: &World, name: &str) -> String {
    let ticket = world.write_ticket(name, "# Work\n");
    let output = world.sloop(&["post", ticket.to_str().unwrap(), "--manual"]);
    assert!(output.status.success());
    let id = World::json_stdout(&output)["data"]["ticket"]["id"]
        .as_str()
        .unwrap()
        .to_owned();
    assert!(world.sloop(&["run", &id]).status.success());
    id
}

fn wait_for_running_agent(world: &World, marker: &str) -> String {
    wait_until("the agent starts", || world.fake_agent_reached(marker));
    let run_id = world.run_id(1);
    wait_until("the run is recorded running", || {
        world.run_state(&run_id) == "running"
    });
    run_id
}

fn wait_for_idle(world: &World) {
    wait_until("the run settles", || {
        let output = world.sloop(&["status"]);
        World::json_stdout(&output)["data"]["gate"]["active_agents"] == 0
    });
}

#[test]
fn a_run_outliving_its_lease_ttl_keeps_its_expiry_moving_forward() {
    let world = World::configured();
    world.configure_fake_agent(FakeAgent::new().block_until_released("long").exit(0));
    world.commit_all("initial");
    world.start_daemon();
    post_and_run(&world, "long.md");
    let run_id = wait_for_running_agent(&world, "long");

    let claimed_expiry = world
        .lease_expires_at_ms(&run_id)
        .expect("a running run holds a lease");
    // Three passes at two thirds of the TTL each. The run outlives the expiry
    // it was claimed with; only renewal keeps its lease from lapsing.
    let mut previous = claimed_expiry;
    for _ in 0..3 {
        world.tick(Duration::from_millis((LEASE_MS * 2 / 3) as u64));
        wait_until("the reconcile pass renews the lease", || {
            world
                .lease_expires_at_ms(&run_id)
                .is_some_and(|expires| expires > previous)
        });
        let renewed = world.lease_expires_at_ms(&run_id).unwrap();
        assert!(
            renewed > world.now_ms(),
            "the lease lapsed under the daemon: {renewed}"
        );
        previous = renewed;
    }
    assert!(
        world.now_ms() > claimed_expiry,
        "the run did not outlive the expiry it was claimed with"
    );

    world.release("long");
    wait_for_idle(&world);
    assert_eq!(world.run_state(&run_id), "merged");
    // Settlement releases the lease.
    assert_eq!(world.lease_expires_at_ms(&run_id), None);
}

#[test]
fn recovery_re_arms_the_lapsed_lease_of_a_run_that_is_still_alive() {
    let world = World::configured();
    world.configure_fake_agent(FakeAgent::new().block_until_released("survivor").exit(0));
    world.commit_all("initial");
    let daemon_pid = world.start_daemon()["data"]["pid"].as_u64().unwrap() as u32;
    post_and_run(&world, "survivor.md");
    let run_id = wait_for_running_agent(&world, "survivor");
    let agent_pid = world.run_process_id(&run_id);

    // The daemon dies, then the clock passes the TTL while the agent lives on.
    world.kill_daemon(daemon_pid);
    world.tick(Duration::from_millis(LEASE_MS as u64 + 1_000));
    let lapsed_at = world.now_ms();
    assert!(
        world.lease_expires_at_ms(&run_id).unwrap() < lapsed_at,
        "the lease should have lapsed while the daemon was down"
    );
    assert!(process_alive(agent_pid), "the agent outlived its daemon");

    // Ordinary renewal could never lift this lease; adoption re-arms it.
    world.start_daemon();
    wait_until("recovery re-arms the adopted run's lease", || {
        world
            .lease_expires_at_ms(&run_id)
            .is_some_and(|expires| expires > lapsed_at)
    });
    let rearmed = world.lease_expires_at_ms(&run_id).unwrap();

    // And the re-armed lease then renews like any other supervised run.
    world.tick(Duration::from_millis(LEASE_MS as u64 - 1_000));
    wait_until("the re-armed lease renews", || {
        world
            .lease_expires_at_ms(&run_id)
            .is_some_and(|expires| expires > rearmed)
    });

    world.release("survivor");
    wait_for_idle(&world);
    // However the readopted run is classified, it settles and releases its
    // lease; re-arming delays neither.
    assert!(
        !["claimed", "running", "aftercare"].contains(&world.run_state(&run_id).as_str()),
        "the readopted run did not settle: {}",
        world.run_state(&run_id)
    );
    assert_eq!(world.lease_expires_at_ms(&run_id), None);
}

#[test]
fn re_arming_never_resurrects_the_lease_of_a_run_that_died() {
    let world = World::configured();
    world.configure_fake_agent(
        FakeAgent::new()
            .commit("agent work")
            .block_until_released("doomed")
            .exit(0),
    );
    world.commit_all("initial");
    let daemon_pid = world.start_daemon()["data"]["pid"].as_u64().unwrap() as u32;
    post_and_run(&world, "doomed.md");
    let run_id = wait_for_running_agent(&world, "doomed");
    let agent_pid = world.run_process_id(&run_id);

    // Kill the daemon, then the agent's own process group, then let the lease
    // lapse with nobody left to renew it.
    world.kill_daemon(daemon_pid);
    world.kill_process_group(agent_pid);
    wait_until("the agent process is gone", || !process_alive(agent_pid));
    world.tick(Duration::from_millis(LEASE_MS as u64 + 1_000));

    world.start_daemon();
    wait_until("recovery settles the orphaned run", || {
        world.run_state(&run_id) == "orphaned"
    });
    // Settlement deleted the lease rather than adoption re-arming it.
    assert_eq!(world.lease_expires_at_ms(&run_id), None);
}
