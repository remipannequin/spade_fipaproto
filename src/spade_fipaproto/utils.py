from enum import StrEnum
import uuid
from spade.message import Message
from spade.template import Template


class Performatives(StrEnum):
    """FIPA performatives used by the protocols"""
    REQUEST = "request"
    QUERY = "query"
    QUERY_IF = "query-if"
    QUERY_REF = "query-ref"
    INFORM = "inform"
    FAILURE = "failure"
    AGREE = "agree"
    REFUSE = "refuse"
    ACCEPT_PROPOSAL = "accept-proposal"
    REFUSE_PROPOSAL = "refuse-proposal"
    NOT_UNDERSTOOD = "not-understood"
    CALL_FOR_PROPOSAL = "call-for-proposal"
    PROPOSE = "propose"


def set_thread(msg: Message) -> tuple[Message, Template]:
    """Set the thread in the message, and return the modified message and the
    corresponding template"""
    # set and record the conversation id ("thread")
    if msg.thread:
        thread = msg.thread
    else:
        thread = str(uuid.uuid1())
        msg.thread = thread
    template = Template()
    template.thread = thread
    return (msg, template)