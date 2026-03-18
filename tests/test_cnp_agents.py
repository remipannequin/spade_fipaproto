"""Test Achieve Rational Effect initiator and Responder, test CNP init and responder."""

import spade
from spade.agent import Agent
from spade.message import Message

from spade_fipaproto import ContractNetInitiator, ContractNetResponder, Performatives as Perf


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

        # receive CFP on a xmpp topic

        self.cnp_responder = ContractNetResponder()
        self.cnp_responder.propose = handle_propose
        self.cnp_responder.handle_accept_prop = handle_accept
        self.add_behaviour(self.cnp_responder) # TODO add filter on protocol metadata ?


class InitiatorTestAg(Agent):
    """Agent to test RE initiator"""

    init_cnp: ContractNetInitiator

    async def setup(self):

        # send CFP on a topic

        # add initiator behaviour
        self.init_cnp = ContractNetInitiator(
            timeout=20,  # if timeout is too high, the CFP from previous exec trigger the responder
            body="How much to open the window ?",
            participants=["responder@localhost"]
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
        await initiator_ag.stop()
        await responder_ag.stop()
        success = True

    
    spade.run(start_ag())
