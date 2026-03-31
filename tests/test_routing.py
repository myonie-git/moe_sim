from wafer_sim import Simulator


def test_single_packet_reaches_destination_with_expected_latency() -> None:
    simulator = Simulator(topology_type="mesh", width=2, height=2)
    delivered_at: list[int] = []
    simulator.inject_message(
        source_tile_id=0,
        destination_tile_id=3,
        size=8,
        injection_time=0,
        payload_tag="packet",
        collective_op_id="op0",
        group_id="group0",
        on_delivered=lambda packet, time: delivered_at.append(time),
    )
    simulator.run()
    packet = next(iter(simulator._packets.values()))
    assert delivered_at == [5]
    assert packet.hop_count == 2
    assert packet.path_taken == [0, 1, 3]
