import typer

def confirm_prompt(prompt_message: str, **kwargs):
    prompt_message = prompt_message + " [Әйе/юк, Yes/no, Да/нет]"
    return typer.prompt(text = prompt_message, value_proc=_cross_language_yes_no, **kwargs, default="Yes")

def prompt(prompt_message: str, **kwargs):
    return typer.prompt(text = prompt_message, **kwargs)


def secho(**kwargs):
    typer.secho(**kwargs)

def _cross_language_yes_no(argument_value):
    """
    Convert user input to boolean value. Supports multiple languages

    :param argument_value:
    :return:
    """
    argument_value = argument_value.strip().lower()
    for c in ["әйе", " yes", "да"]:
        if c.find(argument_value) != -1:
            return True
    return False
