#!/usr/bin/env python3
"""
DK/400 Terminal User Interface

AS/400-inspired green screen interface for job queue management.
"""
from src.dk400.tui.app import DK400App


def main():
    """Entry point for the DK/400 TUI."""
    app = DK400App()
    app.run()


if __name__ == "__main__":
    main()
