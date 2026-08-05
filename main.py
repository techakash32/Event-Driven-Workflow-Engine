"""
==========================================================
Event-Driven Workflow Engine
Author : Akash Nagar
==========================================================
"""

# ==========================================================
# Imports
# ==========================================================

import os
import uuid
import queue
from datetime import datetime
from typing import Callable, Dict, Any, Optional

from dotenv import load_dotenv
from langchain_google_genai import ChatGoogleGenerativeAI

# ==========================================================
# Load Environment Variables
# ==========================================================

load_dotenv()

GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")

if not GOOGLE_API_KEY:
    raise ValueError("GOOGLE_API_KEY not found inside .env file.")

# ==========================================================
# LLM Configuration
# ==========================================================

llm = ChatGoogleGenerativeAI(
    model="gemini-2.5-flash",
    temperature=0.2,
)

# ==========================================================
# Event Queue
# ==========================================================

class SimulatedEventQueue:

    def __init__(self):
        self._queue = queue.Queue()

    def push_event(
        self,
        event_type: str,
        payload: Dict[str, Any],
        event_id: Optional[str] = None,
    ):

        event = {
            "event_id": event_id or str(uuid.uuid4()),
            "event_type": event_type,
            "payload": payload,
            "received_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }

        self._queue.put(event)

        print(
            f"[Listener] Event Received | "
            f"Type={event_type} | "
            f"ID={event['event_id'][:8]}"
        )

        return event

    def listen_and_drain(self):

        events = []

        while not self._queue.empty():
            events.append(self._queue.get())

        return events

    def is_empty(self):

        return self._queue.empty()


# ==========================================================
# Workflow Registry
# ==========================================================

class WorkflowRegistry:

    def __init__(self):

        self._workflows: Dict[str, Callable] = {}

    def register(self, event_type: str, workflow_fn: Callable):

        self._workflows[event_type] = workflow_fn

        print(
            f"[Registry] Registered workflow -> {event_type}"
        )

    def get(self, event_type: str):

        return self._workflows.get(event_type)

    def known_event_types(self):

        return list(self._workflows.keys())


# ==========================================================
# Initialize Components
# ==========================================================

event_queue = SimulatedEventQueue()

workflow_registry = WorkflowRegistry()

print("✅ Event Queue Initialized")

print("✅ Workflow Registry Initialized")

print("✅ Gemini LLM Connected")

# ==========================================================
# Main Function
# ==========================================================

def main():

    print("\n===============================")
    print(" Event-Driven Workflow Engine ")
    print("===============================\n")

    print("System Ready 🚀")


# ==========================================================
# Entry Point
# ==========================================================

if __name__ == "__main__":
    main()