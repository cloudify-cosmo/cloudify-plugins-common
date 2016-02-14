#!/bin/bash -e

curl $(ctx node properties common_builder_script_url) -o ./common-provision.sh && source common-provision.sh

SUDO=""

function install_dependencies() {
    ctx logger info 'Installing necessary dependencies...'

    if  which yum; then
        sudo yum -y install python-devel gcc openssl git libxslt-devel libxml2-devel openldap-devel
        SUDO="sudo"
    elif which apt-get; then
        sudo apt-get update &&
        sudo apt-get -y install build-essential python-dev
        SUDO="sudo"
    else
        ctx logger info 'Probably a Windows machine...'
        return
    fi
    curl --silent --show-error --retry 5 https://bootstrap.pypa.io/get-pip.py | sudo python &&
    sudo pip install pip==7.1.2 --upgrade
}

function install_wagon() {
    ctx logger info 'Installing Wagon...'
    $SUDO pip install wagon==0.3.0
}

function create_wagon() {
    ctx logger info 'Creating Plugin Wagon...'
    $SUDO wagon create -s https://github.com/cloudify-cosmo/$PLUGIN_NAME/archive/$PLUGIN_TAG_NAME.tar.gz -r --validate -v -f
}

PLUGIN_NAME=$(ctx node properties plugin_name)
PLUGIN_TAG_NAME=$(ctx node properties plugin_tag_name)

print_params
install_dependencies &&
install_wagon &&
create_wagon &&
create_md5 "wgn" &&
[ -z ${AWS_ACCESS_KEY} ] || upload_to_s3 "wgn" && upload_to_s3 "md5"

