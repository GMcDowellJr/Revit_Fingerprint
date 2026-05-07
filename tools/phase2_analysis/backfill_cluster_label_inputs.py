from tools.patterns_analysis._archive.backfill_cluster_label_inputs import *  # noqa: F401,F403


if __name__ == "__main__":
    try:
        main()
    except NameError as exc:
        raise SystemExit("No main() available in archived module: backfill_cluster_label_inputs") from exc
