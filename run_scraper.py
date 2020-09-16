import multiprocessing as mp
import traceback
import csv
import os

TMP_DIR = "./tmp"
SEG_FN = "./segments/speech_segments.csv"


def make_temp_segment_files(num_workers):
    """ Reads segments CSV and partitions into multiple CSVs, one for each worker. """

    with open(SEG_FN, 'r') as f:
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

    pool = mp.Pool(num_workers)

    # check if we need to make CSVs for each worker
    if not bool(os.listdir(TMP_DIR)):
        make_temp_segment_files(num_workers)

    for worker_csv_fn in os.listdir('./tmp/'):
        try:
            pool.apply_async(worker, [worker_csv_fn])
        except Exception as e:
            print("Could not start worker for file %s." % worker_csv_fn)
        finally:
            try:
                pool.close()
                pool.join()
            except Exception as e:
                print('Everything is broken. Just give up.')
                exit()


def worker(csv_fn):
    """ Pipe a CSV into a bash script which uses youtube-dl. """
    try:
        command = "cat ./tmp/%s | ./download.sh" % csv_fn
        os.system(command)
    except Exception as e:
        print('worker had an oopsie on file %s' % csv_fn)
        print(traceback.format_exc())
    return


if __name__ == "__main__":
    make_temp_segment_files(20)
    dispatch_workers(20)