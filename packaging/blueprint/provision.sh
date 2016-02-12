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
    $SUDO wagon create -s https://github.com/cloudify-cosmo/$PLUGIN_NAME/archive/$PLUGIN_TAG_NAME.tar.gz -r --validate -v -f
}



# VERSION/PRERELEASE/BUILD must be exported as they is being read as an env var by the cloudify-agent-packager
CORE_TAG_NAME=$(ctx node properties core_tag_name)
COMMON_BUILDER_SCRIPT=$(ctx node properties common_builder_script_url)

GITHUB_USERNAME=$(ctx node properties github_username)
GITHUB_PASSWORD=$(ctx node properties github_password)
AWS_ACCESS_KEY_ID=$(ctx node properties aws_access_key_id)
AWS_ACCESS_KEY=$(ctx node properties aws_access_key)
PLUGIN_NAME=$(ctx node properties plugin_name)
PLUGIN_TAG_NAME=$(ctx node properties plugin_tag_name)

curl $COMMON_BUILDER_SCRIPT -o ./common-provision.sh && source common-provision.sh
export AWS_S3_PATH="org/cloudify3/wagons/$PLUGIN_NAME/$PLUGIN_TAG_NAME"

print_params
install_dependencies &&
install_wagon &&
wagon_create_package &&
create_md5 "wgn" &&
[ -z ${AWS_ACCESS_KEY} ] || upload_to_s3 "wgn" && upload_to_s3 "md5"

