from tools.patterns_analysis._archive.calibrate_join_key_gates import *  # noqa: F401,F403


if __name__ == "__main__":
    try:
        main()
    except NameError as exc:
        raise SystemExit("No main() available in archived module: calibrate_join_key_gates") from exc
