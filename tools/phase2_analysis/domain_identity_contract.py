from tools.patterns_analysis._archive.domain_identity_contract import *  # noqa: F401,F403


if __name__ == "__main__":
    try:
        main()
    except NameError as exc:
        raise SystemExit("No main() available in archived module: domain_identity_contract") from exc
