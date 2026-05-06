from tools.patterns_analysis._archive.apply_join_keys_by_ids import *  # noqa: F401,F403


if __name__ == "__main__":
    try:
        main()
    except NameError as exc:
        raise SystemExit("No main() available in archived module: apply_join_keys_by_ids") from exc
