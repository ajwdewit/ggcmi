import shelve
import os, glob

class JointShelves():
    _shelves = []
    _last_index = 0
    
    def __init__(self, fpath=os.getcwd(), pattern="*.shelve"):
        # Return the shelve files - with the most recent one first
        fn = os.path.join(fpath, pattern)
        files = glob.glob(fn)

        # Sort the files by the name
        #files.sort(key=lambda x: os.path.getmtime(x)) # last modified date
        files.sort(key=lambda x: os.path.basename(x))
        # Note that duplicate keys can exist in different shelves
        # currently only the first key is found and the value is returned.
        # you could sort in reverse order though.
        # files = reversed(files)
        for fn in files:
            self._shelves.append(shelve.open(fn, flag="r"))

    def __getitem__(self, key):
        if not isinstance(key, str):
            msg = "Key should be of type string!"
            raise RuntimeError(msg)
        
        # First try to get it from the shelve used last
        shlv = self._shelves[self._last_index]
        if shlv.has_key(key):
            return shlv[key]
        
        # Otherwise loop over all the shelves
        i = 0
        for shlv in self._shelves:
            if shlv.has_key(key):
                self._last_index = i
                return shlv[key]
            i += 1
        raise KeyError()
    
    def __setitem__(self, key, value):
        raise NotImplementedError()

    def close(self):
        for s in self._shelves:
            if s != None: s.close()
        self._shelves = None
