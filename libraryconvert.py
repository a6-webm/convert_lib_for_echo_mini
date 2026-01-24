#!/bin/python

import os
from os.path import join
import sys
import enum
import shutil
import shlex
import threading
import queue
import subprocess

def ensure_dot_prefix(s):
    if s[0] == ".":
        return s
    else:
        return "." + s

def dir_empty(dirpath):
    try:
        first = next(os.scandir(dirpath))
    except StopIteration:
        return True
    return False

def user_confirm():
    inp = ""
    while not (inp == "y" or inp == "n"):
        inp = input("Begin? (y/N):")
        inp = inp.lower()
        inp = inp or "n"
    return inp == "y"

def replace_ext(fp, ext):
    return os.path.splitext(fp)[0] + ext

def ext(fp):
    return os.path.splitext(fp)[1]

def worker(q, args, stdout_lock):
    while True:
        rel_fp = q.get()
        src_fp = join(args.SOURCE, rel_fp)
        dest_fp = join(args.DEST, replace_ext(rel_fp, args.to_ext))

        convert_cmd = list(args.CONVERT_CMD)
        for i in range(len(convert_cmd)):
            if convert_cmd[i] == "@source":
                convert_cmd[i] = src_fp
            elif convert_cmd[i] == "@dest":
                convert_cmd[i] = dest_fp

        if args.dry_run:
            with stdout_lock:
                print(shlex.join(convert_cmd))
        else:
            with stdout_lock:
                print("Converting:", src_fp, sep="")
            os.makedirs(os.path.split(dest_fp)[0], exist_ok=True)
            proc = subprocess.run(
                convert_cmd,
                encoding="utf-8",
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
            )
            if proc.returncode != 0 or args.verbose:
                with stdout_lock:
                    print(proc.stdout)

        q.task_done()

def sorted_filtered_paths(dir, filter_exts):
    out = [
        os.path.relpath(join(dirpath, f), dir)
        for dirpath, dirnames, filenames in os.walk(dir)
        for f in filenames
        if ext(f) in filter_exts
    ]
    out.sort()
    return out

def operations_to_sync(src, dest, exts, dest_ext=None):
    from os.path import getmtime
    if not exts:
        return ([], [])
    create = []
    delete = []
    src_fps = sorted_filtered_paths(src, exts)
    len_src_fps = len(src_fps)
    src_i = 0
    dest_fps = sorted_filtered_paths(dest, [dest_ext] if dest_ext else exts)
    len_dest_fps = len(dest_fps)
    dest_i = 0

    while src_i < len_src_fps and dest_i < len_dest_fps:
        src_fp_srt = src_fps[src_i]
        src_fp = src_fp_srt
        if dest_ext:
            src_fp_srt = replace_ext(src_fp_srt, dest_ext)
        dest_fp = dest_fps[dest_i]

        # src and dest both present
        if src_fp_srt == dest_fp: 
            if (
                (not args.ignore_mtime)
                and getmtime(join(src, src_fp)) > getmtime(join(dest, dest_fp))
            ):
                create.append(src_fp)
            src_i += 1
            dest_i += 1
        # dest file not present in src
        elif src_fp_srt > dest_fp:
            delete.append(dest_fp)
            dest_i += 1
        # src file not present dest
        else:
            create.append(src_fp)
            src_i += 1

    # leftover src files
    for src_fp in src_fps[src_i:]:
        create.append(src_fp)
    # leftover dest files
    for dest_fp in dest_fps[dest_i:]:
        delete.append(dest_fp)

    return (create, delete)

def main(args):
    if "@source" not in args.CONVERT_CMD or "@dest" not in args.CONVERT_CMD:
        print("Error: CONVERT_CMD does not contain '@source' and '@dest'")
        exit(1)
    
    if args.jobs <= 0:
        args.jobs += os.cpu_count()
    if args.jobs <= 0:
        args.jobs = 1

    args.to_ext = ensure_dot_prefix(args.to_ext)
    args.from_exts = [ensure_dot_prefix(ext) for ext in args.from_exts.split(",")]
    args.copy_exts = [ensure_dot_prefix(ext) for ext in args.copy_exts.split(",")] \
        if args.copy_exts else []

    files_to_convert, files_to_delete = operations_to_sync(args.SOURCE, args.DEST, args.from_exts, args.to_ext)
    files_to_copy, files_to_delete2 = operations_to_sync(args.SOURCE, args.DEST, args.copy_exts)
    files_to_delete.extend(files_to_delete2)
    del files_to_delete2

    for f in files_to_delete:
        print("delete :", f, sep="")
    for f in files_to_copy:
        print("copy   :", f, sep="")
    for f in files_to_convert:
        print("convert:", f, sep="")

    if args.dry_run:
        print("-----DRY-RUN-----")
    else:
        if (not args.noconfirm) and (not user_confirm()):
            return
        print("-----SYNCING-----")

    for rel_fp in files_to_delete:
        fp = join(args.DEST, rel_fp)
        print("Removing:", fp, sep="")
        if args.dry_run: continue
        os.remove(fp)
        dir = os.path.split(fp)[0]
        if dir_empty(dir):
            os.removedirs(dir)

    for rel_fp in files_to_copy:
        src_fp = join(args.SOURCE, rel_fp)
        dest_fp = join(args.DEST, rel_fp)
        print("Copying:", src_fp, sep="")
        os.makedirs(os.path.split(dest_fp)[0], exist_ok=True)
        if args.dry_run: continue
        shutil.copy(src_fp, dest_fp)

    q = queue.Queue()
    stdout_lock = threading.Lock()

    threads = [
        threading.Thread(
            target=worker, args=(q, args, stdout_lock), daemon=True
        )
        for i in range(args.jobs)
    ]
    for thread in threads:
        thread.start()

    try:
        for rel_fp in files_to_convert:
            q.put(rel_fp)

        q.join()
    except:
        with stdout_lock:
            print("Interrupted")

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("-j", "--jobs", default=0, type=int, help="How many threads to use, or if negative how many threads not to use from the max.")
    parser.add_argument("-d", "--dry-run", action="store_true")
    parser.add_argument("--noconfirm", action="store_true")
    parser.add_argument("-v", "--verbose", action="store_true")
    parser.add_argument("-f", "--from", dest="from_exts", help="Comma separated list of extensions to convert from.", required=True)
    parser.add_argument("-t", "--to", dest="to_ext", help="Extension to convert to.", required=True)
    parser.add_argument("-c", "--copy", dest="copy_exts", help="Comma separated list of extensions to copy directly.")
    parser.add_argument("--ignore-mtime", action="store_true")
    parser.add_argument("SOURCE", help="Path to your library.")
    parser.add_argument("DEST", help="Path where converted files should end up.")
    parser.add_argument("CONVERT_CMD", nargs="*", help="Command for converting, precede with '--'. Should use '@source' and '@dest' instead of paths.")

    args = parser.parse_args()

    main(args)
