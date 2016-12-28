#!/bin/bash -e

SUDO=""

function print_plugins_params() {

    echo "## print common parameters"

    declare -A params=( ["PLUGIN_NAME"]=$PLUGIN_NAME ["PLUGIN_TAG_NAME"]=$PLUGIN_TAG_NAME  \
                        ["PLUGIN_S3_FOLDER"]=$PLUGIN_S3_FOLDER )
    for param in "${!params[@]}"
    do
            echo "$param - ${params["$param"]}"
    done
}

function install_dependencies(){
    echo "## Installing necessary dependencies"

    if  which yum; then
        sudo yum -y install python-devel gcc openssl git libxslt-devel libxml2-devel openldap-devel libffi-devel openssl-devel
    elif which apt-get; then
        sudo apt-get update &&
        sudo apt-get -y install build-essential python-dev
    else
        echo 'probably windows machine'
        pip install virtualenv
        return
    fi
    curl --silent --show-error --retry 5 https://bootstrap.pypa.io/get-pip.py | sudo python &&
    sudo pip install pip==7.1.2 --upgrade
    sudo pip install virtualenv
}

function install_wagon(){
    echo "## installing wagon"
    virtualenv env
    source env/bin/activate
    if  which yum; then
        echo 'redaht/centos machine'
    elif which apt-get; then
        echo 'ubuntu/debian machine'
    else
        echo 'probably windows machine'
    fi
    pip install wagon==0.3.2
}

function wagon_create_package(){

    echo "## wagon create package"
    if [[ "$PLUGIN_NAME" =~ "vsphere" ]] || [[ "$PLUGIN_NAME" =~ "softlayer" ]]; then
        git clone https://$GITHUB_USERNAME:$GITHUB_PASSWORD@github.com/cloudify-cosmo/$PLUGIN_NAME.git
        pushd $PLUGIN_TAG_NAME
            git checkout -b $PLUGIN_TAG_NAME origin/$PLUGIN_TAG_NAME
        popd
        mkdir create_wagon ; cd create_wagon
        wagon create -s ../$PLUGIN_NAME/
    else
        wagon create -s https://github.com/cloudify-cosmo/$PLUGIN_NAME/archive/$PLUGIN_TAG_NAME.tar.gz -r --validate -v -f
    fi
}



# VERSION/PRERELEASE/BUILD must be exported as they is being read as an env var by the cloudify-agent-packager
CORE_TAG_NAME="4.0m11"
curl https://raw.githubusercontent.com/cloudify-cosmo/cloudify-packager/$CORE_TAG_NAME/common/provision.sh -o ./common-provision.sh &&
source common-provision.sh


GITHUB_USERNAME=$1
GITHUB_PASSWORD=$2
AWS_ACCESS_KEY_ID=$3
AWS_ACCESS_KEY=$4
PLUGIN_NAME=$5
PLUGIN_TAG_NAME=$6
PLUGIN_S3_FOLDER=$7


export AWS_S3_PATH="org/cloudify3/wagons/$PLUGIN_NAME/$PLUGIN_S3_FOLDER"

install_common_prereqs &&
print_plugins_params
install_dependencies &&
install_wagon &&
wagon_create_package &&
create_md5 "wgn" &&
[ -z ${AWS_ACCESS_KEY} ] || upload_to_s3 "wgn" && upload_to_s3 "md5"

