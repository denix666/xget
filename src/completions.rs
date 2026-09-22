use anyhow::{Result, bail};

pub fn generate(shell: &str) -> Result<()> {
    match shell {
        "bash" => print!("{}", BASH),
        "zsh" => print!("{}", ZSH),
        "fish" => print!("{}", FISH),
        _ => bail!("unsupported shell: {shell} (use bash, zsh, or fish)"),
    }
    Ok(())
}

const BASH: &str = r#"
_xget() {
    local cur prev
    COMPREPLY=()
    cur="${COMP_WORDS[COMP_CWORD]}"
    prev="${COMP_WORDS[COMP_CWORD-1]}"

    case "$prev" in
        --reget)
            compopt -o filenames
            local IFS=$'\n'
            COMPREPLY=($(compgen -W "$(xget --history-urls 2>/dev/null)" -- "$cur"))
            return 0
            ;;
        --completions)
            COMPREPLY=($(compgen -W "bash zsh fish" -- "$cur"))
            return 0
            ;;
        -o|--output|-i|--input)
            compopt -o filenames
            local IFS=$'\n'
            COMPREPLY=($(compgen -f -- "$cur"))
            return 0
            ;;
        -t|--threads)
            COMPREPLY=($(compgen -W "1 2 4 8 16" -- "$cur"))
            return 0
            ;;
    esac

    if [[ "$cur" == -* ]]; then
        COMPREPLY=($(compgen -W "--help --version --threads --output --input --history --clear-history --reget --completions --verbose" -- "$cur"))
    else
        compopt -o filenames
        local IFS=$'\n'
        COMPREPLY=($(compgen -f -- "$cur"))
    fi
}
complete -o bashdefault -F _xget xget
"#;

const ZSH: &str = r#"
#compdef xget

_xget_history_urls() {
    local urls
    urls=(${(f)"$(xget --history-urls 2>/dev/null)"})
    compadd -a urls
}

_xget() {
    _arguments \
        '1:url:_files' \
        '-h[Show help]' \
        '--help[Show help]' \
        '-V[Show version]' \
        '--version[Show version]' \
        '-v[Verbose logging]' \
        '--verbose[Verbose logging]' \
        '-t[Download threads]:threads:(1 2 4 8 16)' \
        '--threads[Download threads]:threads:(1 2 4 8 16)' \
        '-o[Output path]:output:_files' \
        '--output[Output path]:output:_files' \
        '-i[Input file]:input:_files' \
        '--input[Input file]:input:_files' \
        '--history[Show download history]' \
        '--clear-history[Clear download history]' \
        '--reget[Re-download from history]:url:_xget_history_urls' \
        '--completions[Generate shell completions]:shell:(bash zsh fish)'
}

_xget "$@"
"#;

const FISH: &str = r#"
complete -c xget -l help -d "Show help"
complete -c xget -l version -d "Show version"
complete -c xget -s v -l verbose -d "Verbose logging"
complete -c xget -s t -l threads -d "Download threads" -x -a "1 2 4 8 16"
complete -c xget -s o -l output -d "Output path" -rF
complete -c xget -s i -l input -d "Input file" -rF
complete -c xget -l history -d "Show download history"
complete -c xget -l clear-history -d "Clear download history"
complete -c xget -l reget -d "Re-download from history" -x -a "(xget --history-urls 2>/dev/null | string collect -N)"
complete -c xget -l completions -d "Generate completions" -x -a "bash zsh fish"
complete -c xget -n '__fish_is_first_arg' -F -d "File to download"
"#;
