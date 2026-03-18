"""Test Achieve Rational Effect initiator and Responder, test CNP init and responder."""

from datetime import datetime, timedelta

import spade
from spade.agent import Agent
from spade.message import Message
from spade.template import Template

from spade_fipaproto import (
    ContractNetInitiator,
    ContractNetResponder,
    Performatives as Perf,
)


class ResponderTestAgent(Agent):
    """A test responder"""

    cnp_responder: ContractNetResponder

    async def setup(self):

        def handle_propose(call: Message) -> Message:
            print(f'responder got call "{call.body}"')
            prop = call.make_reply()
            prop.metadata = prop.metadata.copy()
            prop.set_metadata("performative", Perf.PROPOSE)
            prop.body = "50€"
            return prop

        def handle_accept(accept: Message):
            print(f'responder proposition was accepted: "{accept.body}"')
            inform = accept.make_reply()
            inform.metadata = inform.metadata.copy()
            inform.set_metadata("performative", Perf.INFORM)
            inform.body = "I will close the window shortly."
            return inform

        # receive CFP
        self.cnp_responder = ContractNetResponder()
        self.cnp_responder.propose = handle_propose
        self.cnp_responder.handle_accept_prop = handle_accept
        tmpl = Template(metadata={"performative": Perf.CALL_FOR_PROPOSAL.value})
        self.add_behaviour(self.cnp_responder, template=tmpl)  # TODO add filter on protocol metadata ?


class InitiatorTestAg(Agent):
    """Agent to test RE initiator"""

    init_cnp: ContractNetInitiator

    async def setup(self):

        # send CFP on a topic

        # add initiator behaviour
        self.init_cnp = ContractNetInitiator(
            timeout=20,  # if timeout is too high, the CFP from previous exec trigger the responder
            body="How much to open the window ?",
            participants=["responder@localhost"],
        )
        self.init_cnp.handle_done = lambda m: print(
            f'initiator got final notification: "{m.body}"'
        )
        self.init_cnp.handle_failure = lambda m: print(f"failure: {m.body}")

        def accept(m):
            print(f'initiator got proposition "{m.body}", accepting')
            r = m.make_reply()
            r.metadata = r.metadata.copy()
            r.set_metadata("performative", Perf.ACCEPT_PROPOSAL)
            r.body = "Done Deal !"
            return r

        self.init_cnp.handle_single_proposition = accept
        self.add_behaviour(self.init_cnp)


def test_basic_cnp(spade_container):
    success = False

    async def start_ag():
        """Launch one testing agent."""
        nonlocal success
        responder_ag = ResponderTestAgent("responder@localhost", "pwd")
        await responder_ag.start(auto_register=True)
        initiator_ag = InitiatorTestAg("initiator@localhost", "pwd")
        await initiator_ag.start(auto_register=True)

        # wait for initiator to finish
        await initiator_ag.init_cnp.join()

        init_trace: list[tuple[datetime, Message, str]] = initiator_ag.traces.filter(to="responder@localhost")  # type: ignore
        assert len(init_trace) == 4
        init_trace.sort(key=lambda e: e[0])
        start, m1 = init_trace[0][:2]
        assert m1.thread
        assert m1.sender == "initiator@localhost"
        assert m1.to == "responder@localhost"
        assert "performative" in m1.metadata
        assert m1.metadata["performative"] == Perf.CALL_FOR_PROPOSAL.value
        t_agree, m2 = init_trace[1][:2]
        assert "performative" in m2.metadata
        assert m2.metadata["performative"] == Perf.PROPOSE.value
        assert m2.thread == m1.thread
        m3 = init_trace[2][1]
        assert "performative" in m3.metadata
        assert m3.metadata["performative"] == Perf.ACCEPT_PROPOSAL.value
        assert m3.thread == m1.thread
        m4 = init_trace[3][1]
        assert "performative" in m4.metadata
        assert m4.metadata["performative"] == Perf.INFORM.value
        assert m4.thread == m1.thread

        await initiator_ag.stop()
        await responder_ag.stop()
        success = True

    spade.run(start_ag())
    assert success