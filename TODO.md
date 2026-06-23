# Documentation Review

There are 5 documentation files that are currently included in the
distribution .zip archive:

  - [Toplevel README](README.md)
  - [Toplevel SECURITY](SECURITY.md)
  - [`pg-airman-mcp` plugin README](plugins/pg-airman-mcp/README.md)
  - [BFSI stack README](stacks/bfsi-fraud-detection/README.md)
  - [BFSI stack ARCHITECTURE](stacks/bfsi-fraud-detection/ARCHITECTURE.md)

## Issues

 1. In the [Technology Stack](README.md#technology-stack) section, we
    use the term "Partner" even if there is no formal partnership with
    those organizations. Perhaps we should call them "Components"?

 2. In the [README](README.md) we call EDB Postgres® AI Blueprints a
    "ready-to-deploy reference architecture", while in the
    [SECURITY](SECURITY.md) file we place a disclaimer:

    > **This project is designed for local development and
    > proof-of-concept demonstrations only. It is NOT intended for
    > production use without additional security hardening.**

    The disclaimer is clearer, but it is restricted to security. I
    would also mention High Availability too, moving the disclaimer in
    the toplevel README. For instance:

    > **This project is intended for proof-of-concept demonstrations
    > only; it is NOT intended for production use, which requires
    > additional security hardening, and appropriate modifications to
    > the architecture that enable high availability procedures.**
