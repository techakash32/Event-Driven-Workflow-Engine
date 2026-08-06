# Listen to the Queue and webhook

# llm setup

import getpass
import os
from dotenv import load_dotenv

# Load variables from a .env file (GOOGLE_API_KEY=...) into the environment
load_dotenv()

# Get the API key from environment variables or prompt the user
api_key = os.environ.get("GOOGLE_API_KEY")
if not api_key:
    api_key = getpass.getpass("Enter your Google API key: ")
    os.environ["GOOGLE_API_KEY"] = api_key # Also set it for future reference in the environment

from langchain_google_genai import ChatGoogleGenerativeAI

llm = ChatGoogleGenerativeAI(
    model="gemini-3.5-flash-lite",  # fixed: gemini-2.5-flash is no longer available to new users (404)
    temperature=0.2,
    google_api_key=api_key # Pass the API key explicitly
)

import json
import time
import uuid
import random
import queue
import hashlib
from datetime import datetime
from dataclasses import dataclass , field
from typing import Callable , List , Dict , Any , Optional

class SimulatedEventQueue:

  def __init__(self):
    self._queue:"queue.Queue"= queue.Queue()

  def push_event(self,event_type:str,payload:Dict[str,Any],event_id:Optional[str]=None):
    event={
        "event_id":event_id or str(uuid.uuid4()),
        "event_type":event_type,
        "payload":payload,
        "received_at":datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    }
    self._queue.put(event)
    print(f"[listener] event received: type='{event_type}' id={event['event_id'][:8]} ... ")
    return event



  def listen_and_drain(self):
    events = []
    while not self._queue.empty():
      events.append(self._queue.get())
    return events

  def is_empty(self):
    return self._queue.empty()

event_queue = SimulatedEventQueue()
print("Queue Set up is ready .... ")

class WorkflowRegistery:
  def __init__(self):
    self._workflows: Dict[str, Callable]={}

  def register(self, event_type:str, workflow_fn:Callable):
    self._workflows[event_type]=workflow_fn
    print(f"[Workflow-registery] registered workflow for event_type = '{event_type}'")

  def get(self, event_type:str):
    return self._workflows.get(event_type)

  def known_event_types(self):
    return list(self._workflows.keys())   # Fixed: key() -> keys()


def smart_route_event_type(event_type: str, payload:Dict[str,Any], registry:"WorkflowRegistery") -> Optional[str]:
  known = registry.known_event_types()

  prompt = f'''A system received an event with type "{event_type}" and payload {json.dumps(payload)}.
Known workflow event types are: {known}

which known event type is this closet to? Respond with ONLY the extact matching
string from the known list, or "NONE" if nothing fits.'''

  raw_content = llm.invoke(prompt).content
  # fixed: newer Gemini models can return .content as a list of content
  # blocks instead of a plain string, which broke .strip() directly
  if isinstance(raw_content, str):
    guess = raw_content.strip()
  else:
    guess = "".join(
        part.get("text", "") if isinstance(part, dict) else str(part)
        for part in raw_content
    ).strip()

  if guess in known:
    print(f"[workflow-registry] Gemini smart-route unknown type '{event_type}' -> '{guess}'")
    return guess

  print(f"[workflow-registry] no matching workflow found for '{event_type}' (Gemini said: '{guess}')")
  return None


# ----demo workflow----

def workflow_send_order_confirmation(payload: Dict[str, Any]) -> str:
  order_id = payload.get("order_id","unknown")
  return f"Confirmation email sent for order #{order_id}"


def workflow_notify_payment_failure(payload: Dict[str,Any]) -> str:
  user = payload.get("user","unknown")
  if random.random() < 0.6:
    raise RuntimeError("payment gateway notification service timeout")
  return f"payment failur notifivation sent to {user}"


def workflow_send_welcome_message(payload: Dict[str,Any]) -> str:
  name = payload.get("name","user")
  return f"welcome message sent to {name}"


workflow_registry = WorkflowRegistery()
workflow_registry.register("order.created", workflow_send_order_confirmation)
workflow_registry.register("payemnt.failed", workflow_notify_payment_failure)
workflow_registry.register("user.signup", workflow_send_welcome_message)

print("\nRegisterd workflow:", workflow_registry.known_event_types())

# point-3 ::: IDEMPOTENCYSTORE

class IdempotencyStore:

  def __init__(self):
    self._processed: Dict[str, Dict[str, Any]] = {}

  def already_processed(self, event_id: str) -> bool:
    return event_id in self._processed

  def get_cached_result(self, event_id: str) -> Optional[Dict[str, Any]]:
    return self._processed.get(event_id)

  def mark_processed(self, event_id: str, result: Dict[str, Any]):
    self._processed[event_id] = result

  def stats(self) -> Dict[str, int]:
    return {"total_unique_events_processed": len(self._processed)}


idempotency_store = IdempotencyStore()
print("idempotency_ready")

# point-4 ::: Dead letter handling and retry logic

MAX_RETRIES = 3
BASE_BACKOFF_SECONDS = 0.5


class DeadLetterQueue:

  def __init__(self):
    self.items: List[Dict[str, Any]] = []

  def add(self, event: Dict[str, Any], error: str, attempts: int):
    entry = {
        "event": event,
        "error": error,
        "attempts": attempts,
        "failed_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }
    self.items.append(entry)
    print(
        f"[DLQ] event id={event['event_id'][:8]}.... moved to Dead Letter Queue "
        f"after {attempts} attempts. Reason: {error}"
    )

  def list_items(self) -> List[Dict[str, Any]]:
    return self.items

  def remove(self, event_id: str):
    self.items = [
        i for i in self.items
        if i["event"]["event_id"] != event_id
    ]


dlq = DeadLetterQueue()


def execute_with_retry(
    workflow_fn: Callable,
    event: Dict[str, Any],
    dlq: DeadLetterQueue,
    max_retries: int = MAX_RETRIES,
) -> Dict[str, Any]:

    """
    --> Dead-letter handling + retry logic <---
    Exponential backoff ke sath retry karna hai;
    sab attempts fail hone par DLQ me daal deta hai.
    """

    last_error = None
    for attempt in range(1, max_retries + 1):
        try:
            result_text = workflow_fn(event["payload"])
            print(
                f"[retry] attempt {attempt}/{max_retries} succeeded "
                f"for event id={event['event_id'][:8]}..."
            )
            return {
                "status": "success",
                "output": result_text,
                "attempts": attempt,
            }
        except Exception as e:
            last_error = str(e)
        wait_time = BASE_BACKOFF_SECONDS * (2 ** (attempt - 1))
        print(f"[retry] attempt {attempt}/{max_retries} failed: '{last_error}'")
        print(f"-> backing off {wait_time}s before next try")
        time.sleep(wait_time)

    # Saare retries fail ho gaye -> Dead Letter Queue me bhejo
    dlq.add(event, last_error, max_retries)
    return {
        "status": "dead_lettered",
        "error": last_error,
        "attempts": max_retries,
    }


print("Retry + Dead Letter Queue logic ready")