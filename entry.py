import click

from runutils import (runbash, ensure_user, get_user_ids,
                      getvar, ensure_dir, call)


USER_NAME, USER_ID, GROUP_NAME, GROUP_ID = get_user_ids('postgres', 5432)
PGDATA = getvar('PGDATA')
SOCKET_DIR = getvar('SOCKET_DIR')
PG_MAJOR = getvar('PG_MAJOR')


@click.group()
def run():
    ensure_user(USER_NAME, USER_ID, GROUP_NAME, GROUP_ID)
    ensure_dir(PGDATA, owner=USER_NAME, group=GROUP_NAME)
    ensure_dir(SOCKET_DIR, owner=USER_NAME, group=GROUP_NAME,
               permission_str='777')

    # conf = '/usr/share/postgresql/%s/postgresql.conf.sample' % PG_MAJOR
    # hba = '/usr/share/postgresql/%s/pg_hba.conf.sample' % PG_MAJOR
    # copyfile('/postgresql.conf', conf)
    # copyfile('/pg_hba.conf', hba)
    #
    # substitute('/postgresql.conf', {'SOCKET_DIR': SOCKET_DIR,
    #                                 'LOG_DIR': LOG_DIR})


@run.command()
@click.argument('user', default=USER_NAME)
def bash(user):
    runbash(user)


@run.command()
def start():
    call(['ls', '-al', PGDATA], user=USER_NAME)


if __name__ == '__main__':
    run()
