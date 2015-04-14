import conv_settings
import sys
sys.path.append(conv_settings.pcse_dir)
import os, logging
from sqlalchemy import engine as sa_engine
from cropinforeader import CropInfoProvider
from joint_shelves import JointShelves


class CropSimOutputWorker():
    _crop_no = 0
    _crop_name = ""
    _crop_label = ""
    _mgmt_code = ""
    _model = ""
    _climate = ""
    _clim_scenario = ""
    _sim_scenario = ""
    _timestep = "annual"
    _start_year = 1945
    _end_year = 2045
    _db_engine = None
    _crop_info_provider = None 
    _joint_shelves = None
    
    def __init__(self, crop_no, model, climate, clim_scenario, sim_scenario, start_year, end_year):
        # Initialise
        self._crop_no = crop_no
        self._model = model
        self._climate = climate
        self._clim_scenario = clim_scenario
        self._sim_scenario = sim_scenario
        self._start_year = start_year
        self._end_year = end_year 
        try:
            self._db_engine = sa_engine.create_engine(conv_settings.connstr) 
            
            # Retrieve some info about the crop
            self._crop_name, self._crop_label, self._mgmt_code = self._get_crop_info(crop_no)
            self._crop_info_provider = CropInfoProvider(self._crop_name, self._mgmt_code)
            
        except Exception as e:
            print " Error during initialisation of CropSimOutputWorker instance: \n" + str(e)
            raise e
        
    def set_joint_shelves(self, obj):
        if isinstance(obj, JointShelves):
            self._joint_shelves = obj;
        
    def _get_path_to_template(self):
        template_fn = "output_template_{clim_lc}_{timestep}_{start_year}_{end_year}.nc4"
        template_fn = template_fn.format(clim_lc=self._climate.lower(), timestep=self._timestep,
                                         start_year=self._start_year, end_year=self._end_year)
        return os.path.join(conv_settings.results_folder, template_fn)

    def _get_output_filename_pattern(self):
        ncdfname_templ = "{model}_{climate}_{clim_scenario}_{sim_scenario}_{variable}_{crop}_{timestep}_{start_year}_{end_year}.nc4"
        if self._mgmt_code == 'rf': simcode = self._sim_scenario + "_noirr"
        else: simcode = self._sim_scenario + "_firr"
        result = ncdfname_templ.format(model=self._model, climate=self._climate.lower(), clim_scenario=self._clim_scenario,
                                       sim_scenario=simcode, variable="*", crop=self._crop_label,
                                       timestep=self._timestep, start_year=self._start_year, end_year=self._end_year)
        return result
    
    def _get_length_of_season(self, doh, dom, dos, task_id):
        if doh or dom:
            # doh or dom is specified - assume date objects
            f = lambda x, y, z: (x-z).days if x else (y-z).days
            return f(doh, dom, dos) 
        else:
            crop_no, lon, lat = self._get_task_info(task_id)
            if crop_no != self._crop_no:
                raise ValueError("The crop number found for task %s is not as expected" % task_id)
            start_doy, end_doy = self._crop_info_provider.getSeasonDates(float(lon), float(lat))
            timerdata = self._crop_info_provider.getTimerData(start_doy, end_doy, dos.year)
            result = (timerdata['CROP_END_DATE'] - dos).days
            if result < 0: result = result + 365
            return result
        
    def close(self):
        self._db_engine = None
        self._joint_shelves.close()
        self._joint_shelves = None
        del self._joint_shelves
    
    def _get_crop_info(self, crop_no):
        crop_name = ""
        crop_label = ""
        mgmt_code = ""
        conn = self._db_engine.connect()
        sqlStr = """SELECT m.crop_no, x.name, x.label, m.mgmt_code 
                 FROM crop m INNER JOIN cropinfo x ON m.crop_no = x.crop_no
                 WHERE m.crop_no=%s"""
        rows = conn.execute(sqlStr % crop_no)
        for row in rows:
            crop_name = row["name"]
            crop_label = row["label"]
            mgmt_code = row["mgmt_code"]
            break
        conn.close()
        return crop_name, crop_label, mgmt_code

    def _get_task_info(self, taskId):
        crop_no = 0
        lon = 180.0
        lat = 90.0
        conn = self._db_engine.connect()
        sqlStr = """select t.crop_no, t.longitude, t.latitude
                 from tasklist t where t.task_id =""" + str(taskId)
        rows = conn.execute(sqlStr)
        for row in rows:
            crop_no = row["crop_no"]
            lon = row["longitude"]
            lat = row["latitude"]
            break
        conn.close()
        return crop_no, lon, lat
    
    def _get_finished_tasks(self, crop_no):
        result = []
        conn = self._db_engine.connect()
        sqlStr = """SELECT task_id FROM tasklist
                 WHERE crop_no=%s AND status='Finished'"""
        rows = conn.execute(sqlStr % crop_no)
        conn.close()
        for row in rows:
            result.append(row["task_id"])
        return result
    
    def _get_simresult(self, task_id):
        try:
            key_fmt = "%010i"
            return self._joint_shelves[key_fmt % task_id]
        except KeyError:
            msg = "No results found for task_id %s " % task_id
            print msg
            logging.warn(msg)
            return None
        