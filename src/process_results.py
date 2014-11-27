import cPickle
import glob
import run_settings
import os, sys
import time
import shelve
from datetime import datetime
import logging


def get_pkl_files():
    """"Get PCSE pickle files based on template and location
    specified in run_settings.
    """
    # Get the pattern from the template
    fname, ext = (run_settings.output_file_template % 0).split('.')
    pattern = fname.rstrip('0') + "*." + ext

    fn = os.path.join(run_settings.output_folder, pattern)
    return sorted(glob.glob(fn))


class RotatingShelve(object):
    """Stores the content of PCSE output files a shelve.

    PCSE/WOFOST produces many small output files for GGCMI.
    To avoid ending up with 1M files on the file sytem, these
    files are stored in a shelve. This class takes care of
    opening/closing the shelve and rotating to a new one
    when more than max_keys files have been stored in a given
    shelve.
    """
    # counter for keys, max nr of keys and the key format.
    _key_count = 0
    _max_keys = 100000
    _key_fmt = "%010i"
    # the current shelve filename, the path to the shelves, the shelve name
    # format and the handle to the shelve.
    _current_shelve_fname = ""
    _shelve_path = None
    _shelve_fname_fmt = "ggcmi_results_%Y%m%d_%H%M%S.shelve"
    _handle = None
    # if _cleanup then the pickle files are deleted
    _cleanup = False


    def __init__(self, shelve_path=None, max_keys=100000, cleanup=False):
        """Init and start initial shelve.

        Keywords:
        shelve_path : open shelves here
        max_keys : max number of keys stored in a shelve
        cleanup : delete files that are shelved from the file system
        """
        self._max_keys = max_keys
        self._cleanup = cleanup
        if shelve_path is not None:
            self._shelve_path = shelve_path

        shelve_fn = datetime.now().strftime(self._shelve_fname_fmt)
        self._current_shelve_fname = os.path.join(self._shelve_path, shelve_fn)

    def _get_new_shelve(self):
        """Opens a new shelve and returns a handle to it
        """
        shelve_fn = datetime.now().strftime(self._shelve_fname_fmt)
        self._current_shelve_fname = os.path.join(self._shelve_path, shelve_fn)
        handle = shelve.open(self._current_shelve_fname)
        return handle

    def _to_shelve(self, key, obj):
        """"Store given obj under given key in the current shelve."""
        if self._handle is None:
            self._handle = shelve.open(self._current_shelve_fname)
        self._handle[key] = obj
        self._key_count += 1

        if self._key_count >= self._max_keys:
            self._handle.close()
            self._key_count = 0
            self._handle = self._get_new_shelve()

    def close(self):
        if self._handle is not None:
            self._handle.close()
            self._handle = None

    def store_PCSE_files(self, fnames):
        """Stores pickled PCSE files in fnames under the task_id in the current shelve."""
        for fname in fnames:
            pcse_obj = cPickle.load(open(fname, "rb"))
            key = self._key_fmt % pcse_obj["task_id"]
            self._to_shelve(key, pcse_obj)

            if self._cleanup:
                os.remove(fname)

        # Close shelve in order to flush objects to disk
        self.close()


def main():

    log_fname = datetime.now().strftime("ggcmi_output_loader_%Y%m%d_%H%M%S.log")
    log_fname = os.path.join(run_settings.output_folder, log_fname)
    logging.basicConfig(filename=log_fname, format='%(asctime)s %(message)s',
                        level=logging.INFO)

    print "Start storing pickle files in shelves ..."

    rotating_shelve = RotatingShelve(shelve_path=run_settings.shelve_folder,
                                     max_keys=100000, cleanup=True)
    sleep_duration_in_secs = 30
    try:
        while True:
            fnames = get_pkl_files()
            if fnames:
                rotating_shelve.store_PCSE_files(fnames)
                msg = "Written %i files to shelve." % len(fnames)
                logging.info(msg)
                print msg
            else:
                msg = "No PCSE output files detected at: %s" % run_settings.output_folder
                logging.info(msg)
                print msg
            time.sleep(sleep_duration_in_secs)
    except KeyboardInterrupt:
        msg = "Terminating on user request"
        logging.error(msg)
        print msg
        sys.exit()
    except Exception:
        msg = "General error: see log for traceback."
        logging.exception(msg)
    finally:
        rotating_shelve.close()
        logging.shutdown()



if __name__ == "__main__":
    main()
