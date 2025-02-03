import importlib
import click


# From https://click.palletsprojects.com/en/stable/complex/#defining-the-lazy-group
class LazyGroup(click.Group):
    """Command group subclass to lazily load subcommands
    """
    lazy_subcommands: dict[str, str]
    """Map of command names to module paths of the form:

        {command-name} -> {module-name}.{command-object-name}
    """
    def __init__(
        self,
        *args,
        lazy_subcommands: dict[str, str]|None=None,
        **kwargs
    ) -> None:
        super().__init__(*args, **kwargs)
        self.lazy_subcommands = lazy_subcommands or {}

    def list_commands(self, ctx):
        base = super().list_commands(ctx)
        lazy = sorted(self.lazy_subcommands.keys())
        return base + lazy

    def get_command(self, ctx, cmd_name):
        if cmd_name in self.lazy_subcommands:
            return self._lazy_load(cmd_name)
        return super().get_command(ctx, cmd_name)

    def _lazy_load_all(self):
        # This is necessary for sphinx-click to work
        for cmd_name in self.lazy_subcommands.copy().keys():
            cmd_object = self._lazy_load(cmd_name)
            del self.lazy_subcommands[cmd_name]
            self.add_command(cmd_object, name=cmd_name) # type: ignore

    def _lazy_load(self, cmd_name):
        # lazily loading a command, first get the module name and attribute name
        import_path = self.lazy_subcommands[cmd_name]
        modname, cmd_object_name = import_path.rsplit(".", 1)
        # do the import
        mod = importlib.import_module(modname)
        # get the Command object from that module
        cmd_object = getattr(mod, cmd_object_name)
        # check the result to make debugging easier
        if not isinstance(cmd_object, click.BaseCommand):
            raise ValueError(
                f"Lazy loading of {import_path} failed by returning "
                "a non-command object"
            )
        return cmd_object
