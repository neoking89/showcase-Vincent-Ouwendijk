import os
import datetime
import psutil
import pickle
import pandas as pd

def make_path_if_nonexistent(paths: list):
    '''create path if it doesnt exist.'''
    for path in paths:
        if not os.path.exists(path):
            os.makedirs(path)


def get_results(param: str, filepath: str, metric: str = 'Sharpe ratio'):
    '''get best results of a parameter. only 1 chosen parameter can be selected.'''
    d = {}

    pickle_files = os.listdir(filepath)
    for p in pickle_files:
        try:
            with open(filepath + '/' + p, 'rb') as f:
                data = pickle.load(f)
            key = data['params'][param]
            val = data['perf_stats'][metric]
            if key not in d.keys():
                d.update({key: []})
                d[key].append(val)
            else:
                d[key].append(val)
        except:
            continue

    res = pd.DataFrame([(k, sum(v)/len(v)) for k, v in d.items()],
                       columns=[param, metric])
    # res = pd.Series({k: sum(v)/len(v)
    #                 for k, v in d.items()}).sort_values(ascending=False)
    return res


def write_stats_to_dict(filepath: str) -> dict:
    '''loop through all picklefiles in a directory and get chosen parameters with respective ranges as dict'''

    p_dicts = []

    for p in os.listdir(filepath):
        try:
            with open(filepath + '/' + p, 'rb') as f:
                a = pickle.load(f)
        except:
            continue
        p_dicts.append(a['params'])

    # exclude bools, lists and strings! df should only contain numeric values.
    df = pd.DataFrame(p_dicts).select_dtypes(exclude=['object', 'bool'])
    df = df[[i for i in df if len(set(df[i])) > 1]].drop_duplicates().to_dict()
    return {k: list(v.values()) for k, v in df.items()}


def move_picklefiles_to_new_dir(basepath: str) -> str:
    '''create a new directory and move all picklefiles to this directory. return new directoryname.'''

    new_dir = basepath + '/' + datetime.datetime.now().strftime("%Y_%m_%d_%H%M")
    os.makedirs(new_dir)

    iter_base = os.listdir(basepath)

    for i in iter_base:
        file = basepath + '/' + i
        try:
            with open(file, 'rb') as f:
                a = pickle.load(f)
            destination = new_dir + '/' + i
            os.replace(file, destination)
        except:  # catch all non-picklefiles.
            continue

    return new_dir


def get_params(filename):
    '''unwrap param_tuple to get chosen parameters.'''
    with open(filename, 'rb') as f:
        a = pickle.load(f)
        return [i[0] for i in a[0]]


def remove_paths(*paths: str):
    '''remove all paths inserted, if it exists.'''
    for p in paths:
        if os.path.exists(p):
            os.remove(p)
            print('removed', p)


def write_vars_to_txt(*args, filename: str):
    with open(filename, "w") as text_file:
        for arg in args:
            text_file.write(arg)


def clean_gridsearch_path(path: str, total_seconds: int):
    '''
    helperfunction to keep path 'my_logs/backtest/grid_search' clean.
    Removes first files it encouters which differ more than (total_seconds) in creationtime compared to the most recent.
    '''

    def get_creationtime(path):
        epoch = os.path.getctime(path)
        return datetime.datetime.fromtimestamp(epoch)

    d = [i for i in os.listdir(path)[::-1] if i[:4].isnumeric()]

    prev_creationtime = get_creationtime(path + '/' + d[0])

    for idx, file in enumerate(d):
        file = path + '/' + file
        creationtime = get_creationtime(file)
        diff = prev_creationtime - creationtime
        if diff.total_seconds() > total_seconds:
            break
        prev_creationtime = creationtime

    [os.remove(path + '/' + f) for f in d if not f in d[:idx]]
    print(f'amount files left in {path} is {len(d)}')


def get_processes():
    '''returns process IDs matching 'python.exe', sorted on time created.'''
    dict_pids = {
        p.info["pid"]: p.info["name"]
        for p in psutil.process_iter(attrs=["pid", "name"])
    }

    pids = []

    for k, v in dict_pids.items():
        if 'python.exe' in v:
            p = psutil.Process(k)
            pids.append([p.pid, p.create_time()])

    return sorted(pids, key=lambda x: (x[1]))


def kill_last_python_process():
    '''kills last started pythonprocess. Preferably use as last line in a running program.'''
    last_python_pid = get_processes()[-1][0]
    p = psutil.Process(last_python_pid)
    p.terminate()
