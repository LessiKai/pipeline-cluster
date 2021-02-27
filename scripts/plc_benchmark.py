import os
import argparse
import json




if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("directories", nargs="+", type=str)

    args = parser.parse_args()

    for d in args.directories:
        assert os.path.isdir(d)
        benchmarks = []
        benchmark_files = [os.path.join(d, f) for f in os.listdir(d) if os.path.isfile(os.path.join(d, f))]
        for f in benchmark_files:
            with open(f, "r") as fd:
                try:
                    benchmarks.append(json.load(fd))
                except:
                    print("could not load " + f)
        
        merged_benchmarks = []
        for bm in benchmarks:
            if not merged_benchmarks:
                merged_benchmarks = bm
            else:
                for i, t in enumerate(bm):
                    merged_benchmarks[i]["processed"] += t["processed"]
                    merged_benchmarks[i]["time"] += t["time"]

        benchmark_id = int(os.path.basename(d))

        print("--- summary benchmark " + str(benchmark_id) + " ---")
        print("#workers: " + str(len(benchmark_files)))
        for task in merged_benchmarks:
            print()
            print("taskname: " + task["task"])
            print("processed: " + str(task["processed"]))
            print("time: " + str(task["time"] / len(benchmark_files)))
            print("avg time: " + str(task["time"] / task["processed"]))
            
