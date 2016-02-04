#!/bin/bash -e

SUDO=""

function install_dependencies(){
    echo "## Installing necessary dependencies"

    if  which yum; then
        sudo yum -y install python-devel gcc openssl git libxslt-devel libxml2-devel openldap-devel
        SUDO="sudo"
    elif which apt-get; then
        sudo apt-get update &&
        sudo apt-get -y install build-essential python-dev
        SUDO="sudo"
    else
        echo 'probably windows machine'
        return
    fi
    curl --silent --show-error --retry 5 https://bootstrap.pypa.io/get-pip.py | sudo python &&
    sudo pip install pip==7.1.2 --upgrade
}

function install_wagon(){
    echo "## installing wagon"
    $SUDO pip install wagon==0.3.0
}

function wagon_create_package(){
    echo "## wagon create package"
    $SUDO wagon create -s https://$GITHUB_USERNAME:$GITHUB_PASSWORD@github.com/cloudify-cosmo/$PLUGIN_NAME/archive/$PLUGIN_TAG_NAME.tar.gz -r --validate -v -f
}



# VERSION/PRERELEASE/BUILD must be exported as they is being read as an env var by the cloudify-agent-packager
CORE_TAG_NAME="3.4m2"
curl https://raw.githubusercontent.com/cloudify-cosmo/cloudify-packager/$CORE_TAG_NAME/common/provision.sh -o ./common-provision.sh &&
source common-provision.sh


GITHUB_USERNAME=$1
GITHUB_PASSWORD=$2
AWS_ACCESS_KEY_ID=$3
AWS_ACCESS_KEY=$4
PLUGIN_NAME=$5
PLUGIN_TAG_NAME=$6

export AWS_S3_PATH="org/cloudify3/wagons/$PLUGIN_NAME/$PLUGIN_TAG_NAME"

print_params
install_dependencies &&
install_wagon &&
wagon_create_package &&
create_md5 "wgn" &&
[ -z ${AWS_ACCESS_KEY} ] || upload_to_s3 "wgn" && upload_to_s3 "md5"

