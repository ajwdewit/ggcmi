import shelve
import os, glob

class JointShelves():
    _shelves = []
    
    def open(self, fpath=os.getcwd(), pattern="*.shelve"):
        self.__init__(fpath, pattern)

    def __init__(self, fpath=os.getcwd(), pattern="*.shelve"):
        # Return the shelve files - with the most recent one first
        fn = os.path.join(fpath, pattern)
        files = glob.glob(fn)

        # Sort the files by the last modified date
        files.sort(key=lambda x: os.path.getmtime(x))
        files = reversed(files)
        for fn in files:
            self._shelves.append(shelve.open(fn))

    def __getitem__(self, key):
        for shlv in self._shelves:
            if shlv.has_key(key):
                return shlv[key]
        raise KeyError()
    
    def __setitem__(self, key, value):
        raise NotImplementedError()

    def close(self):
        for s in self._shelves:
            s.close()
        self._shelves = None
