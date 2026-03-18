"""Test Achieve Rational Effect initiator and Responder, test CNP init and responder."""
from datetime import timedelta
from datetime import datetime as dt

import spade
from spade.agent import Agent
from spade.message import Message
from spade.template import Template
from spade.behaviour import TimeoutBehaviour

from spade_fipaproto import AchieveREResponder, AchieveREInitiator, Performatives as Perf


class ResponderTestAgent(Agent):
    """A test responder"""
    responder_bh: AchieveREResponder

    async def setup(self):
        class SimulateExec(TimeoutBehaviour):
            """Simulate executing a process"""

            async def run(self):
                assert self.agent
                rq = self.agent.get("current_request")
                await self.agent.responder_bh.done(rq)
                self.agent.set("current_request", None)

        def handle_request(request: Message):
            self.set("current_request", request)
            print(f"responder got a request to {request.body}")
            agree = request.make_reply()
            agree.metadata = agree.metadata.copy()
            agree.set_metadata("performative", Perf.AGREE)
            agree.body = "Okay."
            # add behaviour that will simulate an inform-done
            self.add_behaviour(SimulateExec(dt.now() + timedelta(seconds=5)))
            return agree

        template_rq = Template()
        template_rq.set_metadata("performative", Perf.REQUEST)
        self.responder_bh = AchieveREResponder()
        self.responder_bh.handle_request = handle_request
        self.add_behaviour(self.responder_bh, template_rq)


class InitiatorTestAg(Agent):
    """Agent to test RE initiator"""

    init_re: AchieveREInitiator

    async def setup(self):
        # specific initiator
        class TestInitiator(AchieveREInitiator):
            """Initiator that just print the message it receive"""
            def on_agree(self, agree):
                print(f"AchieveRE received an agree message: {agree}")

            def on_inform(self, inform):
                print(f"AchieveRE received an inform message: {inform}")

            def on_failure(self, failure):
                print(f"AchieveRE received a failure message: {failure}")

            def on_refuse(self, refuse):
                print(f"AchieveRE received an refuse message: {refuse}")

            async def on_end(self):
                print("Achieve RE initiator ending")
        # connect to MQTT
        
        # create a request message
        rq = Message(to="responder@localhost")
        rq.body = "Close the window !"
        rq.set_metadata("performative", Perf.REQUEST)
        # add initiator behaviour
        self.init_re = TestInitiator(rq)
        self.add_behaviour(self.init_re)




def test_basic_interaction(spade_container):
    success = False
    async def start_ag():
        """Launch one testing agent."""
        nonlocal success
        responder_ag = ResponderTestAgent("responder@localhost", "pwd")
        await responder_ag.start(auto_register=True)
        initiator_ag = InitiatorTestAg("initiator@localhost", "pwd")
        await initiator_ag.start(auto_register=True)

        # wait for initiator to finish
        await initiator_ag.init_re.join()
        # test if responder got the request

        # test if initiator got replies: agree, then inform-done

        
        await initiator_ag.stop()
        await responder_ag.stop()
        success = True
    
    spade.run(start_ag())
    assert success