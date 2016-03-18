import os
import subprocess
import signal
import hashlib
from contextlib import contextmanager

import click

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


@contextmanager
def running_db():
    """
    Starts and stops postgres (if it is not running) so the block
    inside the with statement can execute command against it.
    """

    subproc = subprocess.Popen(
        START_POSTGRES,
        preexec_fn=setuser(USER_NAME),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE)

    while True:
        logline = subproc.stderr.readline()
        click.echo(logline)
        if logline.find(b'lock file "postmaster.pid" already exists') > -1:
            subproc.wait()
            subproc = None
            break
        if logline.find(b'ready to accept connections') > -1:
            click.echo('Database is now running ...')
            break

    try:
        yield
    finally:
        if subproc:
            subproc.send_signal(signal.SIGTERM)
            click.echo('Waiting for database to stop...')
            subproc.wait()

    # try:
    #     o, e = subproc1.communicate(timeout=5)
    #     click.echo('subproc1 terminated within 5 seconds.')
    # except subprocess.TimeoutExpired:
    #     subproc2 = subprocess.Popen(
    #         START_POSTGRES,
    #         preexec_fn=setuser(USER_NAME),
    #         stdout=subprocess.PIPE,
    #         stderr=subprocess.PIPE)
    #     for i in range(10):
    #         logline = subproc2.stderr.readline()
    #         click.echo(b'subproc2: ' + logline)
    #
    # subproc = None
    # if not os.path.isfile(os.path.join(PGDATA, 'postmaster.pid')):
    #     subproc = subprocess.Popen(
    #         START_POSTGRES,
    #         preexec_fn=setuser(USER_NAME),
    #         stdout=subprocess.PIPE,
    #         stderr=subprocess.PIPE)
    #
    #     click.echo('Waiting for database to start...')
    #     while True:
    #         logline = subproc.stderr.readline()
    #         if logline.find(b'ready to accept connections') > -1:
    #             break
    #
    # try:
    #     yield
    # finally:
    #     if subproc:
    #         subproc.send_signal(signal.SIGTERM)
    #         click.echo('Waiting for database to stop...')
    #         subproc.wait()


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


@click.group()
def run():
    ensure_user(USER_NAME, USER_ID, GROUP_NAME, GROUP_ID)
    ensure_dir(PGDATA_PARENT, permission_str='777')
    ensure_dir(SOCKET_DIR, permission_str='777')
    if SEMAPHORE_PARENT:
        ensure_dir(SEMAPHORE_PARENT, permission_str='777')

    copyfile('/postgresql.conf', CONF_FILE,
             owner=USER_NAME, group=GROUP_NAME, permission_str='400')
    copyfile('/pg_hba.conf', HBA_FILE,
             owner=USER_NAME, group=GROUP_NAME, permission_str='400')

    substitute(CONF_FILE, {'SOCKET_DIR': SOCKET_DIR, 'HBA_FILE': HBA_FILE})

    if not os.path.isdir(PGDATA):
        call(['initdb'], user=USER_NAME)

    with running_db():
        # set password for admin user
        _setpwd(USER_NAME, MAIN_USER_PWD)

        for k, v in os.environ.items():
            prefix = 'DB_PASSWORD_'
            if k.startswith(prefix):
                username = k[len(prefix):].lower()
                password = v
                try:
                    _createuser(username, password)
                except:
                    _setpwd(username, password)

            prefix = 'DB_OWNER_'
            if k.startswith(prefix):
                dbname = k[len(prefix):].lower()
                owner = v
                try:
                    _createdb(dbname, owner)
                except:
                    pass


@run.command()
@click.argument('user', default=USER_NAME)
def bash(user):
    runbash(user)


@run.command()
def start():
    run_daemon(START_POSTGRES, user=USER_NAME, semaphore=SEMAPHORE)


if __name__ == '__main__':
    run()
