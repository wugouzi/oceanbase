# Enable Powerlevel10k instant prompt. Should stay close to the top of ~/.zshrc.
# Initialization code that may require console input (password prompts, [y/n]
# confirmations, etc.) must go above this block; everything else may go below.
if [[ -r "${XDG_CACHE_HOME:-$HOME/.cache}/p10k-instant-prompt-${(%):-%n}.zsh" ]]; then
  source "${XDG_CACHE_HOME:-$HOME/.cache}/p10k-instant-prompt-${(%):-%n}.zsh"
fi

# If you come from bash you might have to change your $PATH.
# export PATH=$HOME/bin:/usr/local/bin:$PATH

# Path to your oh-my-zsh installation.
export ZSH="$HOME/.oh-my-zsh"

# Set name of the theme to load --- if set to "random", it will
# load a random theme each time oh-my-zsh is loaded, in which case,
# to know which specific one was loaded, run: echo $RANDOM_THEME
# See https://github.com/ohmyzsh/ohmyzsh/wiki/Themes
ZSH_THEME="powerlevel10k/powerlevel10k"

# Set list of themes to pick from when loading at random
# Setting this variable when ZSH_THEME=random will cause zsh to load
# a theme from this variable instead of looking in $ZSH/themes/
# If set to an empty array, this variable will have no effect.
# ZSH_THEME_RANDOM_CANDIDATES=( "robbyrussell" "agnoster" )

# Uncomment the following line to use case-sensitive completion.
# CASE_SENSITIVE="true"

# Uncomment the following line to use hyphen-insensitive completion.
# Case-sensitive completion must be off. _ and - will be interchangeable.
# HYPHEN_INSENSITIVE="true"

# Uncomment one of the following lines to change the auto-update behavior
# zstyle ':omz:update' mode disabled  # disable automatic updates
# zstyle ':omz:update' mode auto      # update automatically without asking
# zstyle ':omz:update' mode reminder  # just remind me to update when it's time

# Uncomment the following line to change how often to auto-update (in days).
# zstyle ':omz:update' frequency 13

# Uncomment the following line if pasting URLs and other text is messed up.
# DISABLE_MAGIC_FUNCTIONS="true"

# Uncomment the following line to disable colors in ls.
# DISABLE_LS_COLORS="true"

# Uncomment the following line to disable auto-setting terminal title.
# DISABLE_AUTO_TITLE="true"

# Uncomment the following line to enable command auto-correction.
# ENABLE_CORRECTION="true"

# Uncomment the following line to display red dots whilst waiting for completion.
# You can also set it to another string to have that shown instead of the default red dots.
# e.g. COMPLETION_WAITING_DOTS="%F{yellow}waiting...%f"
# Caution: this setting can cause issues with multiline prompts in zsh < 5.7.1 (see #5765)
# COMPLETION_WAITING_DOTS="true"

# Uncomment the following line if you want to disable marking untracked files
# under VCS as dirty. This makes repository status check for large repositories
# much, much faster.
# DISABLE_UNTRACKED_FILES_DIRTY="true"

# Uncomment the following line if you want to change the command execution time
# stamp shown in the history command output.
# You can set one of the optional three formats:
# "mm/dd/yyyy"|"dd.mm.yyyy"|"yyyy-mm-dd"
# or set a custom format using the strftime function format specifications,
# see 'man strftime' for details.
# HIST_STAMPS="mm/dd/yyyy"

# Would you like to use another custom folder than $ZSH/custom?
# ZSH_CUSTOM=/path/to/new-custom-folder

# Which plugins would you like to load?
# Standard plugins can be found in $ZSH/plugins/
# Custom plugins may be added to $ZSH_CUSTOM/plugins/
# Example format: plugins=(rails git textmate ruby lighthouse)
# Add wisely, as too many plugins slow down shell startup.
plugins=(git)

source $ZSH/oh-my-zsh.sh

# User configuration

# export MANPATH="/usr/local/man:$MANPATH"

# You may need to manually set your language environment
# export LANG=en_US.UTF-8

# Preferred editor for local and remote sessions
# if [[ -n $SSH_CONNECTION ]]; then
#   export EDITOR='vim'
# else
#   export EDITOR='mvim'
# fi

# Compilation flags
# export ARCHFLAGS="-arch x86_64"

# Set personal aliases, overriding those provided by oh-my-zsh libs,
# plugins, and themes. Aliases can be placed here, though oh-my-zsh
# users are encouraged to define aliases within the ZSH_CUSTOM folder.
# For a full list of active aliases, run `alias`.
#
# Example aliases
# alias zshconfig="mate ~/.zshrc"
# alias ohmyzsh="mate ~/.oh-my-zsh"

# To customize prompt, run `p10k configure` or edit ~/.p10k.zsh.
[[ ! -f ~/.p10k.zsh ]] || source ~/.p10k.zsh

# Regular Colors
export Black='\033[0;30m'        # Black
export Red='\033[0;31m'          # Red
export Green='\033[0;32m'        # Green
export Yellow='\033[0;33m'       # Yellow
export Blue='\033[0;34m'         # Blue
export Purple='\033[0;35m'       # Purple
export Cyan='\033[0;36m'         # Cyan
export White='\033[0;37m'        # White
export NC='\033[0m'              # No Color

log_info() {
  printf "${Red}[$*]${NC}\n"
}

export OBD_INSTALL_PRE=~/.oceanbase-all-in-one/obd
source $OBD_INSTALL_PRE/etc/profile.d/obd.sh
export OBCLIENT_HOME=~/.oceanbase-all-in-one/obclient
export PATH=$OBD_INSTALL_PRE/usr/bin:$OBCLIENT_HOME/u01/obclient/bin:$HOME/.cargo/bin:$PATH
# export PATH=/root/cmake/bin:$PATH

export OB_ROOT=$HOME/oceanbase
export OB_DEBUG_ROOT=$OB_ROOT/build_debug
export OB_RELEASE_ROOT=$OB_ROOT/build_release
export TARGET_CSV=$HOME/archieve/10m.csv
alias cddebug='cd $OB_DEBUG_ROOT'
alias cdrelease='cd $OB_RELEASE_ROOT'
alias cdroot='cd $OB_ROOT'
alias m='log_info MAKE && cddebug && make -j8 && (make install DESTDIR=. || true)'
alias dep='log_info DEPLOY && cddebug && obd mirror create -n oceanbase-ce -V 4.0.0.0 -p ./usr/local/ -f -t final_2022 && obd cluster autodeploy final_2022 -c ../final_2022.yaml -f'
alias res='log_info RESTART && obd cluster restart final_2022'
alias des='(log_info DESTROY && obd cluster destroy -f final_2022) || true'
alias d='des && dep'
alias md='m && d'

alias zcc='source ~/oceanbase/.zshrc'
alias chmodd='chmod 777 $TARGET_CSV'
alias obc='log_info OBC START && chmodd && obclient -h127.0.0.1 -P2881 -uroot -Doceanbase'
alias b='cd /root && log_info BEGIN TEST $TARGET_CSV && chmodd && chmodd && cdroot && obc < bench.sql'
alias mmm='tmux a -t mmm'
alias clash='cd archieve/clash && ./clash-linux-amd64-v1.10.0 -f glados.yaml -d .'
alias zc="vim ~/.zshrc && source ~/.zshrc"
alias mwdk="tmux new -s wdk"
alias wdk="tmux a -t wdk"
alias gpm='git pull && m'

export RELEASE_TARGET_CSV='/root/archieve/demo.csv'
alias chmoddr='chmod 777 $RELEASE_TARGET_CSV'
alias br='cd /root && log_info BEGIN TEST && chmoddr && chmoddr && cdroot && obc < bench_rel.sql'
alias mr='log_info MAKE && cdrelease && make -j8 && (make install DESTDIR=. || true)'
alias depr='log_info DEPLOY && cdrelease && obd mirror create -n oceanbase-ce -V 4.0.0.0 -p ./usr/local/ -f -t final_2022 && obd cluster autodeploy final_2022 -c ../final_2022.yaml -f'
alias dr='des && depr'
alias mdr='mr && dr'


export LOG_ROOT='/data/final/final_2022/log/'
alias log='cd $LOG_ROOT && rg MMMMM observer.log'
alias logm='cd $LOG_ROOT && tail -f observer.log | grep MMMMM'

alias mdb='m && d && b && logm'
alias mdbr='mr && dr && logm'

alias pc="proxychains4"
alias zc="vim ~/.zshrc && source ~/.zshrc"
