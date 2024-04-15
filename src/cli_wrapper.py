import typer


def confirm_prompt(prompt_message: str, **kwargs):
    """
    Prompt user for confirmation with supports of multiple languages. This is basically a wrapper around typer.prompt

    :param prompt_message: the message to show to the user
    :param kwargs:  the same as typer.prompt
    :return: the boolean value of the user input
    """
    prompt_message = prompt_message + " [Әйе/юк, Yes/no, Да/нет]"
    return typer.prompt(text=prompt_message, value_proc=_cross_language_yes_no, **kwargs, default="Yes")


def prompt(prompt_message: str, **kwargs):
    """
    Prompt user for input. This is basically a wrapper around typer.prompt

    :param prompt_message: the message to show to the user
    :param kwargs: the same as typer.prompt
    :return: the boolean value of the user input
    """
    return typer.prompt(text=prompt_message, **kwargs)


def secho(**kwargs):
    """
    Wrapper around typer.secho

    :param kwargs: the same as typer.secho
    """
    typer.secho(**kwargs)


def _cross_language_yes_no(argument_value):
    argument_value = argument_value.strip().lower()
    for c in ["әйе", " yes", "да"]:
        if c.find(argument_value) != -1:
            return True
    return False
