import click

from runutils import runbash, ensure_user, get_user_ids


USER_NAME, USER_ID, GROUP_NAME, GROUP_ID = get_user_ids('dev', 1000)


@click.group()
def run():
    ensure_user(USER_NAME, USER_ID, GROUP_NAME, GROUP_ID)


@run.command()
@click.argument('user', default=USER_NAME)
def bash(user):
    runbash(user)


@run.command()
def start():
    click.echo('Started...')


if __name__ == '__main__':
    run()
