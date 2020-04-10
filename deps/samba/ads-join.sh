#!/bin/bash

# EFSSMB_* overrides
AD_USERNAME=${EFSSMB_AD_USERNAME:-Administrator}
AD_PASSWORD=${EFSSMB_AD_PASSWORD:-Password!!}
DOMAIN_NAME=${EFSSMB_DOMAIN_NAME:-corp.example.com}
WORKGROUP=${EFSSMB_WORKGROUP:-CORP}
NETBIOS_NAME=${EFSSMB_NETBIOS_NAME:-EDGEFS}
DC1=${EFSSMB_DC1:-LOCALDC.${DOMAIN_NAME}}

# calculated vars
ADMIN_SERVER=${ADMIN_SERVER:-${DOMAIN_NAME,,}}
REALM=${REALM:-${DOMAIN_NAME^^}}
PASSWORD_SERVER=${PASSWORD_SERVER:-${ADMIN_SERVER,,}}
SMBCONF=${SMBCONF:-${NEDGE_HOME}/etc/samba/smb.conf}

function ini_val() {
	local file="${1:-}"
	local sectionkey="${2:-}"
	local val="${3:-}"
	local delim=" = "
	local section=""
	local key=""

	# Split on . for section. However, section is optional
	read section key <<<$(IFS="."; echo ${sectionkey})
	if [ -z "${key}" ]; then
		key="${section}"
		section=""
	fi

	local current=$(awk -F "${delim}" "/^${key}${delim}/ {for (i=2; i<NF; i++) printf \$i \" \"; print \$NF}" "${file}")
	if [ -z "${val}" ]; then
		# get a value
		echo "${current}"
	else
		# set a value
		if [ -z "${current}" ]; then
			# doesn't exist yet, add

			if [ -z "${section}" ]; then
				# no section was given, add to bottom of file
				echo "${key}${delim}${val}" >> "${file}"
			else
				# add to section
				sed -i.bak -e "/\[${section}\]/a ${key}${delim}${val}" "${file}"
				# this .bak dance is done for BSD/GNU portability: http://stackoverflow.com/a/22084103/151666
				rm -f "${file}.bak"
			fi
		else
			# replace existing
			sed -i.bak -e "/^${key}${delim}/s/${delim}.*/${delim}${val}/" "${file}"
			# this .bak dance is done for BSD/GNU portability: http://stackoverflow.com/a/22084103/151666
			rm -f "${file}.bak"
		fi
	fi
}

echo "Setting up smb.conf to join ADS \"${DOMAIN_NAME^^}\""

ini_val $SMBCONF "global.security" "ads"
ini_val $SMBCONF "global.workgroup" "$WORKGROUP"
ini_val $SMBCONF "global.netbios name" "$NETBIOS_NAME"
ini_val $SMBCONF "global.realm" "$REALM"
ini_val $SMBCONF "global.password server" "$PASSWORD_SERVER"

echo "Setting up Kerberos realm: \"${DOMAIN_NAME^^}\""

if [[ ! -f /etc/krb5.conf.original ]]; then
    mv /etc/krb5.conf /etc/krb5.conf.original
fi

cat > /etc/krb5.conf << EOL
[logging]
    default = FILE:/var/log/krb5.log 
    kdc = FILE:/var/log/kdc.log 
    admin_server = FILE:/var/log/kadmind.log
[libdefaults]
    default_realm = ${DOMAIN_NAME^^}
    dns_lookup_realm = false
    dns_lookup_kdc = false
[realms]
    ${DOMAIN_NAME^^} = {
        kdc = $(echo ${ADMIN_SERVER,,} | awk '{print $1}')
        admin_server = $(echo ${ADMIN_SERVER,,} | awk '{print $1}')
        default_domain = ${DOMAIN_NAME^^}       
    }
    ${DOMAIN_NAME,,} = {
        kdc = $(echo ${ADMIN_SERVER,,} | awk '{print $1}')
        admin_server = $(echo ${ADMIN_SERVER,,} | awk '{print $1}')
        default_domain = ${DOMAIN_NAME,,}
    }
    ${WORKGROUP^^} = {
        kdc = $(echo ${ADMIN_SERVER,,} | awk '{print $1}')
        admin_server = $(echo ${ADMIN_SERVER,,} | awk '{print $1}')
        default_domain = ${DOMAIN_NAME^^}       
    }
    
[domain_realm]
    .${DOMAIN_NAME,,} = ${DOMAIN_NAME^^}
    ${DOMAIN_NAME,,} = ${DOMAIN_NAME^^}
EOL

echo "Generating Kerberos ticket"

echo $AD_PASSWORD | kinit -V $AD_USERNAME@$REALM

echo "Registering $NETBIOS_NAME to Active Directory: User $AD_USERNAME, DC1 $DC1"

net ads join -U"$AD_USERNAME"%"$AD_PASSWORD" -S $DC1