"""Test implementing some fipa protocols : achieve-RE (fipa-request,
fipa-query, ...)."""

from typing import Callable

from loguru import logger

from spade.behaviour import CyclicBehaviour, FSMBehaviour, State
from spade.message import Message

from .utils import Performatives as Perf, set_thread


class AchieveREInitiator(FSMBehaviour):
    """The initiator is a FSM where first a request or query message is sent,
    then wait for a agree or refuse message, and finaly a inform or failure.
    The performative is validated, not the content of the message."""

    RECEIVE_STATE = "receive_state"
    SEND_STATE = "send_state"

    class SendState(State):
        """State that sends the request."""

        def __init__(self, request):
            State.__init__(self)
            self.request = request
            # set metadata such as performative and protocol here ?

        async def run(self):
            await self.send(self.request)
            self.set_next_state(AchieveREInitiator.RECEIVE_STATE)

    class ReceiveReply(State):
        """State that loop on message reception until the protocol is done."""

        def __init__(self, parent):
            State.__init__(self)
            self.parent = parent

        async def run(self):
            # wait for a reply
            reply = await self.receive(10)
            if not reply:
                self.set_next_state(AchieveREInitiator.RECEIVE_STATE)
            else:
                # next behaviour according to performative
                if reply.get_metadata("performative") == Perf.AGREE.value:
                    self.parent.on_agree(reply)
                    self.set_next_state(AchieveREInitiator.RECEIVE_STATE)
                elif reply.get_metadata("performative") == Perf.REFUSE.value:
                    self.parent.on_refuse(reply)
                    self.set_next_state(AchieveREInitiator.RECEIVE_STATE)
                elif reply.get_metadata("performative") == Perf.INFORM.value:
                    self.parent.on_inform(reply)
                    self.set_next_state("")
                elif reply.get_metadata("performative") == Perf.FAILURE.value:
                    self.parent.on_failure(reply)
                    self.set_next_state("")

    def __init__(self, request: Message):
        """Construct an initiator that will send the request"""
        # record the request
        self.request, self.template = set_thread(request)
        self.set_template(self.template)
        FSMBehaviour.__init__(self)

    def setup(self):
        """Send the request and wait for a reply"""
        self.add_state(
            AchieveREInitiator.SEND_STATE,
            AchieveREInitiator.SendState(self.request),
            initial=True,
        )
        self.add_state(
            AchieveREInitiator.RECEIVE_STATE, AchieveREInitiator.ReceiveReply(self)
        )
        self.add_transition(
            AchieveREInitiator.SEND_STATE, AchieveREInitiator.RECEIVE_STATE
        )
        self.add_transition(
            AchieveREInitiator.RECEIVE_STATE, AchieveREInitiator.RECEIVE_STATE
        )

    def on_agree(self, agree: Message):
        """Called when the responder agreed to service the request"""
        logger.debug(f"on_agree: {agree}")

    def on_refuse(self, refuse: Message):
        """Called when the responder refused to service the request"""
        logger.debug(f"on_agree: {refuse}")

    def on_failure(self, failure: Message):
        """Called when the responder reported a failure when servicing the request"""
        logger.debug(f"on_failure {failure}")

    def on_inform(self, inform: Message):
        """Called when the responder finished servicing the request"""
        logger.debug(f"on_inform {inform}")


class AchieveREResponder(CyclicBehaviour):
    """behaviour implementing the responder role of the fipa-request fipa-query and similar
    interaction protocols.

    """

    def __init__(self):
        CyclicBehaviour.__init__(self)
        self.handle_request: Callable[[Message], Message | None] = lambda msg: None

    async def run(self):

        # wait for a message to arrive
        rq = await self.receive(1)
        if rq:
            # create a reply by calling handle_request
            reply = self.handle_request(rq)
            # send the reply
            if reply:
                # if the reply has the inform performative, mark the interaction as finished
                await self.send(reply)
            else:
                logger.warning("handle request did not return a reply")

    async def done(self, request, failure=False, body=None):
        """Call this to notify of the end of the operation"""
        reply = request.make_reply()
        # make a copy to work around a bug in spade
        reply.metadata = reply.metadata.copy()
        if not failure:
            reply.set_metadata("performative", Perf.INFORM.value)
        else:
            reply.set_metadata("performative", Perf.FAILURE.value)
        if body:
            reply.body = body
        else:
            reply.body = f"Done: {request.body}"
        await self.send(reply)
