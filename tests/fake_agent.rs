mod support;

use support::{FakeAgent, World, wait_until};

fn status(world: &World) -> serde_json::Value {
    let output = world.sloop(&["status"]);
    assert!(
        output.status.success(),
        "status failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    World::json_stdout(&output)["data"].clone()
}

#[test]
fn block_until_released_stops_and_resumes_a_fake_agent() {
    let world = World::configured();
    world.configure_fake_agent(
        FakeAgent::new()
            .block_until_released("frozen")
            .commit("work after release")
            .exit(0),
    );
    world.commit_all("initial");
    world.start_daemon();

    let ticket_path = world.write_ticket("blocked-agent.md", "# Wait for release\n");
    let output = world.sloop(&["post", ticket_path.to_str().unwrap(), "--manual"]);
    assert!(output.status.success());
    let ticket = World::json_stdout(&output)["data"]["ticket"]["id"]
        .as_str()
        .expect("ticket id")
        .to_owned();
    assert!(world.sloop(&["run", &ticket]).status.success());

    wait_until("the fake agent reaches its blocking move", || {
        world.fake_agent_reached("frozen")
    });
    let blocked = status(&world);
    assert_eq!(blocked["gate"]["active_agents"], 1);
    assert_eq!(blocked["tickets"]["claimed"], 1);
    assert_eq!(blocked["tickets"]["merged"], 0);

    world.release("frozen");
    wait_until("the released fake agent completes and exits", || {
        let current = status(&world);
        current["gate"]["active_agents"] == 0 && current["tickets"]["merged"] == 1
    });
}
