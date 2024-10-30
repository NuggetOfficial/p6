"""Pure Python Parallel Process Parameter Perusal,
aka: P⁶. P⁶ is a CLI tool that takes a scripts _main_ function
and generates a parallel processed parameter study from it.
The main paradigms used in this script are:
    - The results shall be saved immediately
    - The CPU shall not stand idle
    - There shall be no dependencies
"""
import importlib
import typing
import time
import sys
import os

from itertools import product
from functools import partial
from types import ModuleType
from enum import Enum

from argparse import (
        ArgumentParser,
        Namespace
        )
from concurrent.futures import (
    ThreadPoolExecutor,
    ProcessPoolExecutor,
    Future
    )

class EXIT(Enum):
    OK = 0
    NO_MODULE_NAMED_Y = 1
    NO_MAIN_IN_MODULE = 2
    WRITE_IN_THREAD_FAILED = 7

def make_parser_for_main(module: ModuleType)-> tuple[ArgumentParser, type|None]:
    "Generate an ArgumentParser for the main function of the <module> object"

    # grab main and get rid of return if it exists
    parser = ArgumentParser(f"parse arguments for {module.__name__}")
    params = module.main.__annotations__.copy()
    rtype  = None
    if "return" in params:
        rtype = params.pop("return")
    
    # generate parser from type annotation of main object
    for arg, dtype in params.items():
        parser.add_argument(
                f"--{arg}", nargs=3,type=dtype,
                metavar=("START","STOP","DELTA"),
                required=True
                )
    return parser, rtype

def float_range(start:float, stop:float, step:float):
    "inclusive range from <start> to <stop> in <step>"
    return [x * step for x in range(int(start/step), int(stop/step)+1)]

def yield_parameter_space(args:Namespace):
    """create a generator that encodes all possible
    combinations of the expanded arguments for the input
    script given the <args> Namespace"""
    compressed_space = vars(args).values()
    space = []
    for cs in compressed_space:
        space.append(float_range(*cs))
    return product(*space) 

def save(state:tuple, result:typing.Any, fp:typing.TextIO):
    "create string given the input <state> and its <result> to write to <fp>"
    line = " ".join([f"{p:.6}" for p in state]) + f" {result:.6}\n"
    try:
        fp.write(line)
    except IOError as e:
        print(e.msg)
        print("Thread failed to write... ")
        return EXIT.WRITE_IN_THREAD_FAILED 

def save_on_complete(future:Future, state:tuple, thread_pool:ThreadPoolExecutor, fp:typing.TextIO):
    """callback upon completed <future> on the input <state> which
    submits a save-to-<fp> task to the <thread_pool>"""
    result = future.result()
    thread_pool.submit(save, state, result, fp)

def main(args:Namespace):
    """run a gridded parameter study on the main
    function in the <args.script>."""
    try:
        mdir = os.getcwd()
        sys.path.append(mdir)
        module = importlib.import_module(args.script)
        sys.path.remove(mdir)
    except ImportError as e:
        print(e.msg)
        print("did you prehaps add .py at the end of the filename?")
        return EXIT.NO_MODULE_NAMED_Y

    assert module

    if not hasattr(module, "main"):
        print(f"no main function found in {module.__name__}")
        return EXIT.NO_MAIN

    parser, rtype = make_parser_for_main(module)
    args, _ = parser.parse_known_args()
    
    if os.path.exists("out.swp"):
        print("resetting swap file...")
        os.remove("out.swp")
    fp = open("out.swp", "a")

    # save one core for me plx
    cores = os.cpu_count()-1 
    t0 = time.process_time()

    thread_pool = ThreadPoolExecutor(max_workers=16)
    with ProcessPoolExecutor(max_workers=cores) as exe:
        futures = []
        print("starting task submission")
        for state in yield_parameter_space(args):
            future = exe.submit(module.main, *state)
            future.add_done_callback(
                        partial(
                            save_on_complete,
                            state=state,
                            thread_pool=thread_pool,
                            fp=fp
                            )
                        )
            futures.append(future)
    
    thread_pool.shutdown(wait=True)
    fp.close()
    print(f"Runtime: {time.process_time()-t0:.3}s")
    print("All tasks done, file closed succesfully")
    return EXIT.OK

if __name__ == "__main__":
    parser = ArgumentParser("study parameters of input script")
    parser.add_argument("script", type=str)
    args, _ = parser.parse_known_args()
    exit(main(args).value)

