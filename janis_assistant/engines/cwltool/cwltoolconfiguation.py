from typing import List


class CWLToolConfiguration:
    def __init__(
        self,
        outdir: str = None,
        parallel: bool = None,
        preserve_environment: str = None,
        rm_container: bool = None,
        leave_container: bool = None,
        tmpdir_prefix: str = None,
        tmp_outdir_prefix: str = None,
        cachedir: str = None,
        rm_tmpdir: bool = None,
        leave_tmpdir: bool = None,
        move_outputs: bool = None,
        leave_outputs: bool = None,
        copy_outputs: bool = None,
        enable_pull: bool = None,
        disable_pull: bool = None,
        rdf_serializer: str = None,
        eval_timeout: str = None,
        print_rdf: bool = None,
        print_dot: bool = None,
        print_pre: bool = None,
        print_deps: bool = None,
        print_input_deps: bool = None,
        pack: bool = None,
        version: bool = None,
        validate: bool = None,
        print_subgraph: bool = None,
        print_targets: bool = None,
        strict: bool = None,
        non_strict: bool = None,
        skip_schemas: bool = None,
        verbose: bool = None,
        quiet: bool = None,
        debug: bool = None,
        timestamps: bool = None,
        js_console: bool = None,
        js_hint_options_file: str = None,
        user_space_docker_cmd: str = None,
        singularity: bool = None,
        no_container: bool = None,
        tool_help: bool = None,
        relative_deps: bool = None,
        enable_dev: bool = None,
        enable_ext: bool = None,
        enable_color: bool = None,
        disable_color: bool = None,
        default_container: str = None,
        no_match_user: bool = None,
        custom_net: str = None,
        add_ga4gh_tool_registry: str = None,
        on_error: bool = None,
        compute_checksum: bool = None,
        relax_path_checks: bool = None,
        make_template: bool = None,
        force_docker_pull: bool = None,
        no_read_only: bool = None,
        overrides: str = None,
        target: str = None,
        cidfile_dir: str = None,
        cidfile_prefix: str = None,
        provenance: str = None,
        orcid: str = None,
        full_name: str = None,
    ):
        self.outdir = outdir
        self.parallel = parallel
        self.preserve_environment = preserve_environment
        self.rm_container = rm_container
        self.leave_container = leave_container
        self.tmpdir_prefix = tmpdir_prefix
        self.tmp_outdir_prefix = tmp_outdir_prefix
        self.cachedir = cachedir
        self.rm_tmpdir = rm_tmpdir
        self.leave_tmpdir = leave_tmpdir
        self.move_outputs = move_outputs
        self.leave_outputs = leave_outputs
        self.copy_outputs = copy_outputs
        self.enable_pull = enable_pull
        self.disable_pull = disable_pull
        self.rdf_serializer = rdf_serializer
        self.eval_timeout = eval_timeout
        self.print_rdf = print_rdf
        self.print_dot = print_dot
        self.print_pre = print_pre
        self.print_deps = print_deps
        self.print_input_deps = print_input_deps
        self.pack = pack
        self.version = version
        self.validate = validate
        self.print_subgraph = print_subgraph
        self.print_targets = print_targets
        self.strict = strict
        self.non_strict = non_strict
        self.skip_schemas = skip_schemas
        self.verbose = verbose
        self.quiet = quiet
        self.debug = debug
        self.timestamps = timestamps
        self.js_console = js_console
        self.js_hint_options_file = js_hint_options_file
        self.user_space_docker_cmd = user_space_docker_cmd
        self.singularity = singularity
        self.no_container = no_container
        self.tool_help = tool_help
        self.relative_deps = relative_deps
        self.enable_dev = enable_dev
        self.enable_ext = enable_ext
        self.enable_color = enable_color
        self.disable_color = disable_color
        self.default_container = default_container
        self.no_match_user = no_match_user
        self.custom_net = custom_net
        self.add_ga4gh_tool_registry = add_ga4gh_tool_registry
        self.on_error = on_error
        self.compute_checksum = compute_checksum
        self.relax_path_checks = relax_path_checks
        self.make_template = make_template
        self.force_docker_pull = force_docker_pull
        self.no_read_only = no_read_only
        self.overrides = overrides
        self.target = target
        self.cidfile_dir = cidfile_dir
        self.cidfile_prefix = cidfile_prefix
        self.provenance = provenance
        self.orcid = orcid
        self.full_name = full_name

    def build_command_line(self, workflow, inputs=None, inlineInputs: List[str] = None):
        command = ["cwltool"]

        for key, value in vars(self).items():
            if value is None:
                continue
            prefix = self.prefix_map[key]
            if isinstance(value, bool):
                if value:
                    command.append(prefix)
            else:
                command.extend([prefix, value])

        command.append(workflow)
        if inputs:
            command.append(inputs)
        if inlineInputs:
            command.extend(inputs)

        return command

    prefix_map = {
        "outdir": "--outdir",
        "parallel": "--parallel",
        "preserve_environment": "--preserve-environment",
        "rm_container": "--rm-container",
        "leave_container": "--leave-container",
        "tmpdir_prefix": "--tmpdir-prefix",
        "tmp_outdir_prefix": "--tmp-outdir-prefix",
        "cachedir": "--cachedir",
        "rm_tmpdir": "--rm-tmpdir",
        "leave_tmpdir": "--leave-tmpdir",
        "move_outputs": "--move-outputs",
        "leave_outputs": "--leave-outputs",
        "copy_outputs": "--copy-outputs",
        "enable_pull": "--enable-pull",
        "disable_pull": "--disable-pull",
        "rdf_serializer": "--rdf-serializer",
        "eval_timeout": "--eval-timeout",
        "print_rdf": "--print-rdf",
        "print_dot": "--print-dot",
        "print_pre": "--print-pre",
        "print_deps": "--print-deps",
        "print_input_deps": "--print-input-deps",
        "pack": "--pack",
        "version": "--version",
        "validate": "--validate",
        "print_subgraph": "--print-subgraph",
        "print_targets": "--print-targets",
        "strict": "--strict",
        "non_strict": "--non-strict",
        "skip_schemas": "--skip-schemas",
        "verbose": "--verbose",
        "quiet": "--quiet",
        "debug": "--debug",
        "timestamps": "--timestamps",
        "js_console": "--js-console",
        "js_hint_options_file": "--js-hint-options-file",
        "user_space_docker_cmd": "--user-space-docker-cmd",
        "singularity": "--singularity",
        "no_container": "--no-container",
        "tool_help": "--tool-help",
        "relative_deps": "--relative-deps",
        "enable_dev": "--enable-dev",
        "enable_ext": "--enable-ext",
        "enable_color": "--enable-color",
        "disable_color": "--disable-color",
        "default_container": "--default-container",
        "no_match_user": "--no-match-user",
        "custom_net": "--custom-net",
        "add_ga4gh_tool_registry": "--add-ga4gh-tool-registry",
        "on_error": "--on-error",
        "compute_checksum": "--compute-checksum",
        "relax_path_checks": "--relax-path-checks",
        "make_template": "--make-template",
        "force_docker_pull": "--force-docker-pull",
        "no_read_only": "--no-read-only",
        "overrides": "--overrides",
        "target": "--target",
        "cidfile_dir": "--cidfile-dir",
        "cidfile_prefix": "--cidfile-prefix",
        "provenance": "--provenance",
        "orcid": "--orcid",
        "full_name": "--full-name",
    }
