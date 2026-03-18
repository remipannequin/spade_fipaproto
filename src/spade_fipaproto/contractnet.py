"""Test implementing some fipa protocols : achieve-RE (fipa-request,
fipa-query, ...)."""

from typing import Callable, overload
from datetime import timedelta
from datetime import datetime as dt
import uuid
import asyncio

from loguru import logger

from spade.behaviour import FSMBehaviour, State
from spade.message import Message
from spade.template import Template

from .utils import Performatives as Perf


class ContractNetInitiator(FSMBehaviour):
    """TODO"""

    SEND_CFP = "send-cfp-state"
    RECEIVE_PROP = "receive-proposition-state"
    SEND_ACCEPTANCE = "send-acceptance"
    RECEIVE_DONE = "receive-done"

    class SendCallMqtt(State):
        """Send a call for proposal on a mqtt topic"""

        def __init__(self, parent: "ContractNetInitiator", body):
            State.__init__(self)
            self.parent = parent
            self.body = body

        async def run(self):
            assert self.agent, "Error, agent is None"
            logger.debug("CNP initiator: sending Call on MQTT")

            props = {
                "reply-to": str(self.agent.jid),
                "reply-before": self.parent.timeout.isoformat(),
                "thread": self.parent.thread,
            }
            await self.agent.mqtt.publish(
                self.parent.topic,
                payload=self.body,
                properties=props,
                retain=True,
            )
            self.set_next_state(ContractNetInitiator.RECEIVE_PROP)

        async def on_start(self):
            assert self.agent
            assert self.agent.mqtt, "The MqttMixin is not available"
            assert (
                self.agent.mqtt.is_connected
            ), "The agent must be connected to a MQTT broker first"

    class SendCallParticipants(State):
        """Send a call for proposal on a mqtt topic"""

        def __init__(self, parent: "ContractNetInitiator", body):
            State.__init__(self)
            self.parent = parent
            self.body = body

        async def run(self):
            assert self.agent, "Error, agent is None"
            logger.debug("CNP initiator: sending Call to a list of agents")
            
            props = {
                "reply-to": str(self.agent.jid),
                "reply-before": self.parent.timeout.isoformat(),
                "thread": self.parent.thread,
            }
            for dest in self.parent.participants:
                msg = Message(to=dest, body=self.body, metadata=props)
                await self.send(msg)
            self.set_next_state(ContractNetInitiator.RECEIVE_PROP)


    class ReceivePropositions(State):
        """Receive and gather all proposition or refusal to propose"""

        def __init__(self, parent: "ContractNetInitiator"):
            """Wait for propositions some time"""
            State.__init__(self)
            self.parent = parent

        async def run(self):
            logger.debug("CNP initiator: Waiting for proposition")
            max_seconds = (self.parent.timeout - dt.now()).seconds
            if max_seconds > 0:
                prop = await self.receive(max_seconds)
                if prop:
                    if prop.get_metadata("performative") == Perf.PROPOSE:
                        # Callback to react to single proposition
                        reply = self.parent.handle_single_proposition(prop)
                        if reply:
                            # a valid reply has been generated
                            self.parent.acceptances.append(reply)
                        else:
                            # no valid reply: save the proposition for later use
                            self.parent.propositions.append(prop)
                    elif prop.get_metadata("performative") == Perf.REFUSE:
                        self.parent.refusals.append(prop)
                    self.set_next_state(ContractNetInitiator.RECEIVE_PROP)
            else:
                if self.parent.propositions or self.parent.acceptances:
                    # we have propositions to handle
                    self.set_next_state(ContractNetInitiator.SEND_ACCEPTANCE)
                elif self.parent.dones:
                    # we just have accepted propositions to get done notifications
                    self.set_next_state(ContractNetInitiator.RECEIVE_DONE)
                # else, initiator finish
                else:
                    logger.debug("no propositions received")

    class SendAcceptances(State):
        """Reply to all propositions after the deadline"""

        def __init__(self, parent):
            State.__init__(self)
            self.parent = parent

        async def run(self):
            self.parent.acceptances.extend(
                self.parent.handle_all_propositions(self.parent.propositions)
            )
            logger.debug(
                f"CNP initiator: sending {len(self.parent.acceptances)} accept/reject messages"
            )
            for acceptance in self.parent.acceptances:
                if acceptance.get_metadata("performative") == Perf.ACCEPT_PROPOSAL:
                    self.parent.dones.append(acceptance)
                await self.send(acceptance)
            if self.parent.dones:  # at least one done
                self.set_next_state(ContractNetInitiator.RECEIVE_DONE)
            else:
                logger.debug("warning no propositions accepted")
                # initiator stops

    class ReceiveDone(State):
        """Receive inform-Done or failure notifications"""

        def __init__(self, parent):
            State.__init__(self)
            self.parent = parent

        async def run(self):
            logger.debug("CNP Initiator: Waiting notification")
            msg = await self.receive(1)
            if msg:
                if msg.get_metadata("performative") == Perf.INFORM:
                    # search a message in done that has been sent
                    accept = self.parent.find_accept(msg)
                    if accept:
                        self.parent.dones.remove(accept)
                        self.parent.handle_done(msg)
                    else:
                        logger.warning(
                            f"CNP Initiator: unexpected notification (no previous accept) {msg}"
                        )
                elif msg.get_metadata("performative") == Perf.FAILURE:
                    self.parent.handle_failure(msg)
                else:
                    logger.warning(f"unexpected message {msg}")

    @overload
    def __init__(self, body, timeout, participants: list[str]): ...
    @overload
    def __init__(self, body, timeout, mqtt_topic: str): ...

    def __init__( # type: ignore
        self,
        body,
        timeout,
        participants: list[str] | None = None,
        mqtt_topic: str | None = None,
    ):
        """Initiate a contract net protocol, by sending a message with the given body either
        to a list of agents, or to a MQTT topic."""    
        if participants:
            self.participants = participants
            self.send_state = self.SendCallParticipants(self, self.body)
        elif mqtt_topic:
            self.topic = mqtt_topic
            self.send_state = self.SendCallMqtt(self, self.body)
        else:
            raise ValueError("One of the argument participants or mqtt_topic must be passed")
        self.thread = str(uuid.uuid1())
        # TODO: it can be an int or float (number of second)
        # a timedelta (relative time) or a datetime (absolute dl)
        # or None : no wait indefinitely
        self.timeout = dt.now() + timedelta(seconds=timeout)
        self.body = body
        # TODO make a version where we wait for a number of reply instead
        self.propositions: list[Message] = []
        self.refusals: list[Message] = []
        self.acceptances: list[Message] = []
        self.dones: list[Message] = []
        # Handle a proposition as soon as it is received
        self.handle_single_proposition: Callable[[Message], Message | None] = (
            lambda p: None
        )
        self.handle_done: Callable[[Message], None] = lambda done: None
        self.handle_failure: Callable[[Message], None] = lambda done: None
        FSMBehaviour.__init__(self)

    def setup(self):

        self.add_state(self.SEND_CFP, self.SendCallMqtt(self, self.body), initial=True)

        self.add_state(self.RECEIVE_PROP, self.ReceivePropositions(self))
        self.add_state(self.SEND_ACCEPTANCE, self.SendAcceptances(self))
        self.add_state(self.RECEIVE_DONE, self.ReceiveDone(self))
        self.add_transition(self.SEND_CFP, self.RECEIVE_PROP)
        self.add_transition(self.RECEIVE_PROP, self.RECEIVE_PROP)
        self.add_transition(self.RECEIVE_PROP, self.SEND_ACCEPTANCE)
        self.add_transition(self.RECEIVE_PROP, self.RECEIVE_DONE)
        self.add_transition(self.SEND_ACCEPTANCE, self.RECEIVE_DONE)
        self.set_template(Template(thread=self.thread))

    def reset(self):
        """Reset the propositions, refusal, dones lists"""
        self.propositions = []
        self.refusals = []
        self.dones = []
        self.acceptances = []

    def find_accept(self, done: Message) -> Message | None:
        """Search an accept message corresponding to a done or failure notification"""
        for accept in self.dones:
            if accept.to == done.sender:
                return accept
        return None

    def handle_all_propositions(self, propositions: list[Message]) -> list[Message]:
        """Handle the propositions when the deadline is over."""
        # Default implementation: refuse all propositions
        replies = []
        for p in propositions:
            r = p.make_reply()
            r.metadata = r.metadata.copy()
            r.set_metadata("performative", Perf.REFUSE)
            replies.append(r)
        return replies


class ContractNetResponder(FSMBehaviour):
    """Respond to call for proposal using the contract net protocol"""

    SUBSCRIBE = "subscribe-state"
    WAIT_CALL = "wait-call-state"
    RESPOND_TO_CALL = "respond-to-cfp-state"
    WAIT_ACCEPT = "wait-accept-state"
    SEND_NOTIFICATION = "send-notification-state"

    class Subscribe(State):
        """Subscribe to MQTT topic where CFP are published"""

        def __init__(self, parent):
            State.__init__(self)
            self.parent = parent

        async def run(self):
            assert self.agent
            assert self.agent.mqtt, "The MqttMixin is not available"
            assert (
                self.agent.mqtt.is_connected()
            ), "The agent must be connected to a MQTT broker first"
            logger.debug(
                f"CNP Responder: Subscribing to MQTT topic {self.parent.topic}"
            )
            await self.agent.mqtt.subscribe(self.parent.topic)
            self.set_next_state(ContractNetResponder.WAIT_CALL)

    class WaitCallMqtt(State):
        """Wait for call for proposal on mqtt topic"""

        def __init__(self, parent):
            State.__init__(self)
            self.parent = parent

        async def run(self):
            assert self.agent and self.agent.mqtt
            logger.debug("CNP Responder: Waiting for MQTT message")
            msg = await self.agent.mqtt.receive(self.parent.topic)
            if msg:
                # Extract the message
                props = dict(msg.properties.UserProperty)
                # Check date
                if "reply-before" in props:
                    deadline = dt.fromisoformat(props["reply-before"])
                    if deadline <= dt.now():
                        # CFP is no longer valid
                        self.set_next_state(ContractNetResponder.WAIT_CALL)
                        return
                cfp = Message(
                    sender=props["reply-to"],
                    to=self.agent.jid,
                    thread=props["thread"],
                    body=msg.payload.decode("UTF-8"),
                )
                cfp.set_metadata("performative", Perf.CALL_FOR_PROPOSAL)
                self.parent.call = cfp
                self.set_next_state(ContractNetResponder.RESPOND_TO_CALL)
            else:
                self.set_next_state(ContractNetResponder.WAIT_CALL)

    class WaitCall(State):
        """Wait for call for proposal message"""

        def __init__(self, parent):
            State.__init__(self)
            self.parent = parent

        async def run(self):
            assert self.agent
            logger.debug("CNP Responder: Waiting for MQTT message")
            cfp = await self.receive(self.parent.topic)
            if cfp:
                # Extract the message
                props = cfp.metadata
                # Check date
                if "reply-before" in props:
                    deadline = dt.fromisoformat(props["reply-before"])
                    if deadline <= dt.now():
                        # CFP is no longer valid: loop on this state
                        self.set_next_state(ContractNetResponder.WAIT_CALL)
                        return
                
                self.parent.call = cfp
                self.set_next_state(ContractNetResponder.RESPOND_TO_CALL)
            else:
                self.set_next_state(ContractNetResponder.WAIT_CALL)

    class RespondToCall(State):
        """Respond to CFP using the message returned by propose"""

        def __init__(self, parent):
            State.__init__(self)
            self.parent = parent

        async def run(self):
            logger.debug("CNP Responder: responding to call")
            prop = self.parent.propose(self.parent.call)
            if prop:
                await self.send(prop)
                if prop.get_metadata("performative") == Perf.PROPOSE:
                    self.set_next_state(ContractNetResponder.WAIT_ACCEPT)
                    # Accept / Refuse message will have the same thread
                    self.parent.set_template(Template(thread=prop.thread))
                else:
                    logger.warning(f"unexpected message {prop}")
                    self.set_next_state(ContractNetResponder.WAIT_CALL)

    class WaitAccept(State):
        """If sending a proposition, wait for a confirmation"""

        def __init__(self, parent):
            State.__init__(self)
            self.parent = parent

        async def run(self):
            logger.debug("CNP responder: Waiting for acceptation....")
            msg = await self.receive(10)
            if msg:
                if msg.get_metadata("performative") == Perf.ACCEPT_PROPOSAL:
                    logger.debug(f"CNP responder proposition accepted: {msg}")
                    inform = self.parent.handle_accept_prop(msg)
                    if inform:
                        await self.send(inform)
                        self.set_next_state(ContractNetResponder.WAIT_CALL)
                    else:
                        self.parent.notification_ev = asyncio.Event()
                        self.set_next_state(ContractNetResponder.SEND_NOTIFICATION)
                elif msg.get_metadata("performative") == Perf.REFUSE_PROPOSAL:
                    logger.debug(f"CNP responder proposition rejected : {msg}")
                    self.parent.handle_reject_prop(msg)
                    self.parent.reset()
                    self.set_next_state(ContractNetResponder.WAIT_CALL)
                else:
                    logger.warning(f"got unexpected {msg}")
            else:
                self.set_next_state(ContractNetResponder.WAIT_ACCEPT)

    class SendNotification(State):
        """Wait for a notification to be send, and go back to wait_call state"""

        def __init__(self, parent):
            State.__init__(self)
            self.parent = parent

        async def run(self):
            # periodically check if a notification has been send
            await self.parent.notification_ev
            self.parent.reset()
            self.set_next_state(ContractNetResponder.WAIT_CALL)

    def __init__(self, topic: str|None = None):
        self.topic = topic
        FSMBehaviour.__init__(self)
        self.propose: Callable[[Message], Message|None] = lambda cfp: None
        self.notification_ev = asyncio.Event()

    def setup(self):
        if self.topic:
            self.add_state(self.SUBSCRIBE, self.Subscribe(self), initial=True)
            self.add_state(self.WAIT_CALL, self.WaitCallMqtt(self))
        else:
            self.add_state(self.WAIT_CALL, self.WaitCall(self), initial=True)
        self.add_state(self.WAIT_ACCEPT, self.WaitAccept(self))
        self.add_state(self.RESPOND_TO_CALL, self.RespondToCall(self))
        self.add_state(self.SEND_NOTIFICATION, self.SendNotification(self))
        self.add_transition(self.SUBSCRIBE, self.WAIT_CALL)
        self.add_transition(self.WAIT_CALL, self.WAIT_CALL)
        self.add_transition(self.WAIT_CALL, self.RESPOND_TO_CALL)
        self.add_transition(self.RESPOND_TO_CALL, self.WAIT_ACCEPT)
        self.add_transition(self.RESPOND_TO_CALL, self.WAIT_CALL)
        self.add_transition(self.WAIT_ACCEPT, self.WAIT_ACCEPT)
        self.add_transition(self.WAIT_ACCEPT, self.WAIT_CALL)
        self.add_transition(self.WAIT_ACCEPT, self.SEND_NOTIFICATION)
        self.add_transition(self.SEND_NOTIFICATION, self.WAIT_CALL)

    def handle_accept_prop(self, accept: Message) -> Message:
        """Handle an acceptation message. Default implementation just reply with a done message."""
        done = accept.make_reply()
        done.set_metadata("performative", Perf.INFORM)
        return done

    def handle_reject_prop(self, disconfirm):
        """Handle a rejection message."""

    async def notify(self, inform):
        """Send a notification message"""
        await self.send(inform)
        self.notification_ev.set()
