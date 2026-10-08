"""Console entry point for `prefect-agentic-workflows-mcp`."""

from prefect_agentic_workflows_mcp.server import build_server


def main() -> None:
    """Run the server over stdio."""
    build_server().run(transport="stdio", show_banner=False)


if __name__ == "__main__":
    main()
