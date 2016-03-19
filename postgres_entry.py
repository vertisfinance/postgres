import os
import subprocess
import signal
import hashlib
from contextlib import contextmanager

import click
import psycopg2

from runutils import (runbash, ensure_user, get_user_ids,
                      getvar, ensure_dir, call, copyfile, substitute,
                      run_daemon, setuser, run_cmd)


USER_NAME, USER_ID, GROUP_NAME, GROUP_ID = get_user_ids('postgres', 5432)
PGDATA = getvar('PGDATA')
PGDATA_PARENT = os.path.split(PGDATA)[0]
SOCKET_DIR = getvar('SOCKET_DIR')
PG_MAJOR = getvar('PG_MAJOR')
MAIN_USER_PWD = getvar('MAIN_USER_PWD')

CONF_BASE = '/usr/share/postgresql/%s/%%s' % PG_MAJOR
CONF_FILE = CONF_BASE % 'postgresql.conf'
HBA_FILE = CONF_BASE % 'pg_hba.conf'
START_POSTGRES = ['postgres', '-c', 'config_file=%s' % CONF_FILE]
SEMAPHORE = getvar('SEMAPHORE', required=False)
if SEMAPHORE:
    SEMAPHORE_PARENT = os.path.split(SEMAPHORE)[0]
else:
    SEMAPHORE_PARENT = None
CONN_STR = "host='%s' dbname='postgres' user=%s password='%s'"
CONN_STR = CONN_STR % (SOCKET_DIR, USER_NAME, MAIN_USER_PWD)


@contextmanager
def running_db():
    """
    Starts and stops postgres (if it is not running) so the block
    inside the with statement can execute command against it.
    """

    subproc = None
    if not os.path.isfile(os.path.join(PGDATA, 'postmaster.pid')):
        click.echo('Starting the dtabase...')
        subproc = subprocess.Popen(
            START_POSTGRES,
            preexec_fn=setuser(USER_NAME),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE)

        click.echo('Waiting for database to start...')
        while True:
            logline = subproc.stderr.readline()
            if logline.find(b'ready to accept connections') > -1:
                break

    try:
        conn = psycopg2.connect(CONN_STR)
    except:
        click.echo('postmaster.pid existed or the database '
                   'started, but could not connect.')
        raise Exception('Database could not be started.')
    else:
        conn.close()

    try:
        yield
    finally:
        if subproc:
            subproc.send_signal(signal.SIGTERM)
            click.echo('Waiting for database to stop...')
            subproc.wait()


def psqlparams(command=None, database='postgres'):
    """Returns a list of command line arguments to run psql."""

    if command is None:
        return ['psql', '-d', database, '-h', SOCKET_DIR]
    else:
        return ['psql', '-d', database, '-h', SOCKET_DIR, '-c', command]


def md5(username, password):
    tomd5 = '%s%s' % (password, username)
    tomd5 = tomd5.encode('utf-8')
    password = hashlib.md5()
    password.update(tomd5)
    password = password.hexdigest()
    return 'md5%s' % password


def _createuser(username, password):
    """Creates a user with the given password."""

    password = md5(username, password)
    sql = "CREATE USER %s WITH PASSWORD '%s'" % (username, password)

    with running_db():
        run_cmd(psqlparams(sql),
                'Creating user %s' % username,
                user=USER_NAME)


def _setpwd(username, password):
    """Sets the password for the given user."""

    password = md5(username, password)
    sql = "ALTER USER %s WITH PASSWORD '%s'" % (username, password)

    with running_db():
        run_cmd(psqlparams(sql),
                'Setting password for %s' % username,
                user=USER_NAME)


def _createdb(dbname, owner):
    """Creates a database."""

    sql = "CREATE DATABASE %s WITH ENCODING 'UTF8' OWNER %s"
    sql = sql % (dbname, owner)

    with running_db():
        run_cmd(psqlparams(sql),
                'Creating database %s' % dbname,
                user=USER_NAME)


def _init(stopper):
    ensure_dir(PGDATA_PARENT, permission_str='777')
    ensure_dir(SOCKET_DIR, permission_str='777')
    if SEMAPHORE_PARENT:
        ensure_dir(SEMAPHORE_PARENT, permission_str='777')

    if stopper.stopped:
        return

    if not os.path.isdir(PGDATA):
        call(['initdb'], user=USER_NAME)

    if stopper.stopped:
        return

    with running_db():
        # set password for admin user
        _setpwd(USER_NAME, MAIN_USER_PWD)

        for k, v in os.environ.items():
            if stopper.stopped:
                return
            prefix = 'DB_PASSWORD_'
            if k.startswith(prefix):
                username = k[len(prefix):].lower()
                password = v
                try:
                    _createuser(username, password)
                except:
                    _setpwd(username, password)

        for k, v in os.environ.items():
            if stopper.stopped:
                return
            prefix = 'DB_OWNER_'
            if k.startswith(prefix):
                dbname = k[len(prefix):].lower()
                owner = v
                try:
                    _createdb(dbname, owner)
                except:
                    pass


@click.group()
def run():
    ensure_user(USER_NAME, USER_ID, GROUP_NAME, GROUP_ID)
    copyfile('/postgresql.conf', CONF_FILE,
             owner=USER_NAME, group=GROUP_NAME, permission_str='400')
    copyfile('/pg_hba.conf', HBA_FILE,
             owner=USER_NAME, group=GROUP_NAME, permission_str='400')

    substitute(CONF_FILE, {'SOCKET_DIR': SOCKET_DIR, 'HBA_FILE': HBA_FILE})


@run.command()
@click.argument('user', default=USER_NAME)
def bash(user):
    runbash(user)


@run.command()
def repair():
    """Rapair stale lock file (postmaster.pid) situations."""
    subproc = subprocess.Popen(
        START_POSTGRES,
        preexec_fn=setuser(USER_NAME),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE)

    success = False
    for i in range(10):
        logline = subproc.stderr.readline()
        if logline.find(b'ready to accept connections') > -1:
            success = True
            break

    subproc.send_signal(signal.SIGTERM)
    subproc.wait()

    if success:
        click.echo('Success.')
    else:
        click.echo('Could not repair :(')


@run.command()
def start():
    run_daemon(START_POSTGRES, user=USER_NAME,
               semaphore=SEMAPHORE, initfunc=_init)


if __name__ == '__main__':
    run()
