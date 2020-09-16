import multiprocessing as mp
import subprocess
import csv
import os

OUT_DIR = "/data2/audioset-speech/files"
TMP_DIR = "./tmp"
SEG_FN = "./segments/speech_segments.csv"
REM_SEG_FN = "./segments/remaining.csv"


def make_remainder_segments_file():
    """ Reads OUT_DIR and SEG_FN and creates new CSV with files left to download. """

    downloaded = os.listdir(OUT_DIR)

    with open(SEG_FN, 'r') as f:
        reader = csv.reader(f)
        with open(REM_SEG_FN, 'w+') as w:
            writer = csv.writer(w)
            for row in reader:
                if any([row[0] in fn for fn in downloaded]):
                    continue
                writer.writerow(row)
    return


def make_temp_segment_files(num_workers):
    """ Reads segments CSV and partitions into multiple CSVs, one for each worker. """

    with open(REM_SEG_FN, 'r') as f:
        reader = csv.reader(f)
        num_entries = sum(1 for _ in reader)
        entries_per_worker = num_entries // num_workers

        # starting index of each worker CSV in the main CSV.
        starts = [entries_per_worker * x for x in range(num_workers)]

        for worker_id, start_idx in enumerate(starts):
            f.seek(0)

            with open("./tmp/tmp_%d.csv" % worker_id, 'w+') as w:
                writer = csv.writer(w)
                count = 0

                for row_idx, row in enumerate(reader):
                    # start writing when we get to current worker's start idx
                    if row_idx >= start_idx:
                        # break when we've written enough for this worker
                        if count > entries_per_worker:
                            break
                        writer.writerow(row)
                        count += 1
                    continue
    return


def dispatch_workers(num_workers=20):
    """ Function to do stuff"""
    workers = []

    # check if we need to make CSVs for each worker
    if not bool(os.listdir(TMP_DIR)):
        make_temp_segment_files(num_workers)

    for worker_csv_fn in os.listdir('./tmp/'):
        try:
            w = mp.Process(target=worker, args=(worker_csv_fn,))
            workers.append(w)
            w.start()

        except Exception as e:
            pass

    for w in workers:
        try:
            w.join()
        except Exception as e:
            exit()


def worker(csv_fn):
    """ Pipe a CSV into a bash script which uses youtube-dl. """
    try:
        command = "cat ./tmp/%s | ./download.sh" % csv_fn
        ps = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        output = ps.communicate()[0]

    except Exception as e:
        pass
    return


if __name__ == "__main__":
    dispatch_workers(25)
