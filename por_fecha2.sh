#
SID=$1
ESQUEMA=$2
TABLA=$3
TIPO_ACTUALIZACION=$4
CAMPO_BASE=$5
ESQUEMA_ORIGEN=$6
TABLA_ORIGEN=$7
CONEXION=$8
HILO=${9}
HERRAMIENTA=${10}
RUTA_LOG=${11}
RUTA_PERSONALIZADO=${12}
ESTADO=${13}
ODB_COPY_OPC=${14}
VSQL_COPY_OPC=${15}
DIAS_POR_FECHA=${16}
FECHA_INICIO=${17}
FECHA_PROCESO=${18}
INC=${19}
TIPO_VALIDACION=${20}
PID_PADRE=${21}
TIPO_BASE_DE_DATOS=${22}
CAMPOS_LLAVE=${23}
CAMPOS_ORIGEN="${24}"
LISTA_CONTRA=${25}
PASS_VERTICA=${26}
USER_VERTICA=${27}
PARAMETROS_ADICIONALES=${28} #NUEVA_O_MODIFICADA
BITACORA_DSN="${29}"
V_BIT_N_ORG_USR="${30}"
V_BIT_X_ORG_USR="${31}"
V_BIT_OWNER="${32}"
SERVIDOR_EJECUTOR="${33}"
CAMPOS_BORRADO="${34}"
TIEMPO_EJEC="${35}"
TABLA_DDL="${36}"
PYTHON_CHUNK=${38}

V_F_FEC_ACT="SYSDATE"

CAMPOS_ORIGEN=$(echo $CAMPOS_ORIGEN | sed -e ''s/P_FCH_INI/$V_F_FEC_ACT/g'')

CAMPOS_ORIGEN=$(echo $CAMPOS_ORIGEN | sed -e ''s/P_FCH_FIN/$V_F_FEC_ACT/g'')

CAMPOS_ORIGEN=$(echo $CAMPOS_ORIGEN | sed -e ''s/SYSDATE/$V_F_FEC_ACT/g'')


CAMPOS_ORIGEN="$CAMPOS_ORIGEN"


FECHA_ACTUALIZA=$(date +%Y-%m-%d --date="-$DIAS_POR_FECHA day")
##############################################################
#################Valida parametros############################
##############################################################
[ -z "$CAMPOS_BORRADO" -o "$CAMPOS_BORRADO" == "NA" ] && CAMPOS_BORRADO="";
[ -z "$TIEMPO_EJEC" -o $TIEMPO_EJEC -eq 0 ] && TIEMPO_EJEC=14400;

IFS='~' read -r -a DELIMITADOR <<< "$HERRAMIENTA"
V_ODB_FS="${DELIMITADOR[1]}"
V_ODB_RS="${DELIMITADOR[2]}"
V_HPV_FS="${DELIMITADOR[3]}"
V_HPV_RS="${DELIMITADOR[4]}"
HERRAMIENTA="${DELIMITADOR[0]}"
#########Obtiene filtros###################
IFS='~' read -r -a LISTA_PARAMETROS <<< "$PARAMETROS_ADICIONALES"
FILTROS="${LISTA_PARAMETROS[0]}"
HINTSS="${LISTA_PARAMETROS[1]}"
FILTROS2="${LISTA_PARAMETROS[2]}"

FILTROS=$(echo $FILTROS | sed -e ''s/P_FCH_INI/$FECHA_ACTUALIZA/g'') 

if [ -z "$FILTROS"  ] || [ "$FILTROS" = "na" ] || [ "$FILTROS" = "NA" ]; then FILTROS=""; fi; echo "filtros $FILTROS";
if [ -z "$HINTSS"  ] || [ "$HINTSS" = "na" ] || [ "$HINTSS" = "NA" ]; then HINTSS=""; fi; echo "hints $HINTSS";

echo "cnx $CONEXION X" 
USUARIO=$(cat $LISTA_CONTRA | awk '$1=="'$CONEXION'" {print($2)}') 
echo "usu $USUARIO U" 
CONTRASENA=$(cat $LISTA_CONTRA | awk '$1=="'$CONEXION'" {print($3)}') 

echo HINTSS $HINTSS
##############################################################
########################Constantes############################
##############################################################
V_T_SCRIPT="por_fecha2.sh"
IFS='~' read -r -a CAMPO_BASE <<< "$CAMPO_BASE"
CAMPO_BASE_ORA="${CAMPO_BASE[0]}"

##############################################################
########################Variables ############################
##############################################################
PID=$$
PARQUE_BASE="/home/m12082/vinkos/CRm/ExL/parque_gen"
PATH_PARQUET_FILES="/mnt/data013/raw_files/rdata_prod"
#####################################
############Auditoria
#####################################
USUARIO_OS_PROCESO=`whoami`
###USUARIO_BD_PROCESO="VERTICA_APP"
USUARIO_BD_PROCESO="USER_V_dB"
NOMBRE_PROCESO="ExtraccNion tabla"
SHELL_PROCESO="por_fecha2.sh"
###SERVER_PROCESO="$SERVIDOR_EJECUTOR"
SERVER_PROCESO='1.1.1.1'
RUTA_PROCESO=`pwd`
ID_PROCESO=$$
###FOLIO_REQ="1707C0753"
FOLIO_REQ="FQ_por_fecha2"

#ODB="/data/ODb/odb64luo"
ODB="odb64luo"

#####################################
#ESCRIBE PARAMETROS EN LOG DE EJECUCION
echo PID_PADRE $PID_PADRE PID $PID FECHA_PROCESO $FECHA_PROCESO INC $INC >> $RUTA_LOG 2>&1

#RESULTADO EJECUTANDO
V_T_Terminal=$($ODB -u ''$V_BIT_N_ORG_USR'' -p ''$V_BIT_X_ORG_USR'' -d $BITACORA_DSN -x "UPDATE $V_BIT_OWNER.TBL_HCH_RESULTADOS_LOBD_2 SET RESULTADO='EJECUTANDO' WHERE PID='$PID_PADRE' and INC=$INC" 2>&1)

#PYTHON_CHUNK
if [ -z "$PYTHON_CHUNK" ]
then
    echo "Does not use chunks"
else
    PYTHON_CHUNK="#chunk=$PYTHON_CHUNK"
    echo "PYTHON_CHUNK = $PYTHON_CHUNK"
fi

#export LD_LIBRARY_PATH=/usr/lib/oracle/11.2/client64/lib:/usr/local/unixODBC/lib
#export LD_LIBRARY_PATH=/home/ad636f/.localopenssl-1.1.0/lib:$LD_LIBRARY_PATH

#function principal {
    #FECHA_INICIO  
    UPDATE_ESTATUS=$($ODB -u ''$V_BIT_N_ORG_USR'' -p ''$V_BIT_X_ORG_USR'' -d $BITACORA_DSN -x  "UPDATE $V_BIT_OWNER.TBL_HCH_RESULTADOS_LOBD_2 SET FECHA_INICIO='$FECHA_INICIO' WHERE PID='$PID_PADRE' and INC=$INC" 2>&1)
    echo FECHA_INICIO $FECHA_INICIO

    TODOS_ORIGEN="NULL"
    TODOS_DESTINO_A="NULL" 
    TODOS_DESTINO_AA="NULL" 
    TODOS_STG="NULL" 
    TODOS_DESTINO="NULL" 
    PARCIAL_ORIGEN="NULL" 
    PARCIAL_DESTINO="NULL"

    #CONTEO PARCIAL ORIGEN
    #PARCIAL_ORIGEN=$(timeout --signal=9 $TIEMPO_EJEC $ODB -u $USUARIO -p $CONTRASENA -d $CONEXION -x "select $HINTSS count(*) from $ESQUEMA_ORIGEN.$TABLA_ORIGEN TBL where $CAMPO_BASE_ORA >= TO_DATE ('$FECHA_ACTUALIZA','YYYY-MM-DD') $FILTROS") 
    PARCIAL_ORIGEN="1"
    echo "PARCIAL_ORIGEN: $PARCIAL_ORIGEN"

    if [ -z $PARCIAL_ORIGEN ] || [ $PARCIAL_ORIGEN -eq 0 ]
    then 
        echo "Advertencia : [1] Esquema [$ESQUEMA_ORIGEN] Tabla[$TABLA_ORIGEN] con cero registros"
        FECHA_FIN=$(date +'%Y-%m-%d %H:%M:%S')
        RESULTADO='ADVERTENCIA'
        V_T_Terminal=$($ODB -u ''$V_BIT_N_ORG_USR'' -p ''$V_BIT_X_ORG_USR'' -d $BITACORA_DSN -x  "UPDATE $V_BIT_OWNER.TBL_HCH_RESULTADOS_LOBD_2 SET  FECHA_FIN='$FECHA_FIN',RESULTADO='$RESULTADO' WHERE PID='$PID_PADRE' and INC=$INC" 2>&1)
        echo FECHA_FIN $FECHA_FIN
        UPDATE_ESTATUS=$($ODB -u ''$V_BIT_N_ORG_USR'' -p ''$V_BIT_X_ORG_USR'' -d $BITACORA_DSN -x  "UPDATE $V_BIT_OWNER.TBL_HCH_RESULTADOS_LOBD_2 SET TODOS_ORIGEN=$TODOS_ORIGEN, PARCIAL_ORIGEN=$PARCIAL_ORIGEN TODOS_DESTINO_A=$TODOS_DESTINO_A, TODOS_DESTINO_AA=$TODOS_DESTINO_AA, TODOS_STG=$TODOS_STG, TODOS_DESTINO=$TODOS_DESTINO, PARCIAL_DESTINO=$PARCIAL_DESTINO WHERE PID='$PID_PADRE' and INC=$INC" 2>&1)
        exit 1
    else
        #LF Delete historical parquet files - there should only be the day file
        RUTA_BORRADO="$PATH_PARQUET_FILES/$ESQUEMA_ORIGEN/$TABLA_ORIGEN/*"
        $(rm -r $RUTA_BORRADO)
        
        echo "Ejecutando ODB" 
        echo "$ODB -u ''$USUARIO'' -p ''xxxxxxxxxxx'' -d $CONEXION -soe -timeout $TIEMPO_EJEC -pcn -e sql=[select $HINTSS $CAMPOS_ORIGEN from $ESQUEMA_ORIGEN.$TABLA_ORIGEN TBL where $CAMPO_BASE_ORA >= TO_DATE ('$FECHA_ACTUALIZA','YYYY-MM-DD') $FILTROS]:tgt=$ODB_COPY_OPC" 
        timeout --signal=9 $(($TIEMPO_EJEC + 60)) $ODB -u ''$USUARIO'' -p ''$CONTRASENA'' -d $CONEXION -soe -timeout $TIEMPO_EJEC -pcn -e "sql=[select $HINTSS $CAMPOS_ORIGEN from $ESQUEMA_ORIGEN.$TABLA_ORIGEN TBL where $CAMPO_BASE_ORA >= TO_DATE ('$FECHA_ACTUALIZA','YYYY-MM-DD') $FILTROS]:tgt=$ODB_COPY_OPC" 2>>$RUTA_LOG | python3 $PARQUE_BASE/parque_loader_v2.py "$ESQUEMA_ORIGEN"."$TABLA_ORIGEN$PYTHON_CHUNK" "$FECHA_PROCESO" "$TABLA_DDL" >> "$RUTA_LOG"
        
        echo "-------------------------------" 
        #Validate ODB output
        ODB_RES=$(cat $RUTA_LOG)
        echo "ODB_RES -> $ODB_RES"
        echo "-------------------------------"
        ODB_RES=$(echo "$ODB_RES" | grep -i "Error\|snapshot too old\|end-of-file on communication channel\|exceeded simultaneous SESSIONS_PER_USER limit\|invalid username/password\|Err:\|Killed\|Received SIG" | grep -v -i "Invalid connection string attribute" | grep -v -i "but no error reporting API found" | grep -v -i "] Source" | grep -v -i "] Target: " | grep -v -i "Native Err: 28002" | grep -v -i "Driver does not support this function (State: IM001" | wc -l)
        
        if [ $ODB_RES -gt 0 ]; then
		      echo "NUM ERRORS: $ODB_RES"
          echo "     Error [6].- Error al extraer tabla [$ESQUEMA_ORIGEN.$TABLA_ORIGEN] en $V_T_SCRIPT"
          FECHA_FIN=$(date +'%Y-%m-%d %H:%M:%S')
          RESULTADO='FRACASO'
          V_T_Terminal=$($ODB -u ''$V_BIT_N_ORG_USR'' -p ''$V_BIT_X_ORG_USR'' -d $BITACORA_DSN -x  "UPDATE $V_BIT_OWNER.TBL_HCH_RESULTADOS_LOBD_2 SET  FECHA_FIN='$FECHA_FIN',RESULTADO='$RESULTADO' WHERE PID='$PID_PADRE' and INC=$INC" 2>&1)
          echo FECHA_FIN $FECHA_FIN
          exit 1
        fi
        
        #PARQUE_ROW_CNT=$(grep "${TABLA_ORIGEN} pnum_rows=" "$RUTA_LOG" | cut -d"=" -f2)
        PARQUE_ROW_CNT=$(grep "pnum_rows=" "$RUTA_LOG" | cut -d"=" -f2)
        echo "<<..parque row count=${PARQUE_ROW_CNT}>>"
        
        echo "Validacion DESTINO - ORIGEN -> if [ $PARQUE_ROW_CNT -ge $PARCIAL_ORIGEN ]"
        if [ $PARQUE_ROW_CNT -ge $PARCIAL_ORIGEN ]
        then 
            echo "Ejecucion exitosa"
            PARCIAL_DESTINO=$PARQUE_ROW_CNT
            echo "PARCIAL_DESTINO $PARCIAL_DESTINO"
            TODOS_STG=$PARQUE_ROW_CNT
            echo "TODOS_STG $TODOS_STG"
            FECHA_FIN=$(date +'%Y-%m-%d %H:%M:%S')
            RESULTADO='EXITO'  
            V_T_Terminal=$($ODB -u ''$V_BIT_N_ORG_USR'' -p ''$V_BIT_X_ORG_USR'' -d $BITACORA_DSN -x  "UPDATE $V_BIT_OWNER.TBL_HCH_RESULTADOS_LOBD_2 SET  FECHA_FIN='$FECHA_FIN',RESULTADO='$RESULTADO' WHERE PID='$PID_PADRE' and INC=$INC" 2>&1)
            echo FECHA_FIN $FECHA_FIN
        else
            echo "Error [1].- Generacion archivo parquet"
            FECHA_FIN=$(date +'%Y-%m-%d %H:%M:%S')
            RESULTADO='FRACASO'
            V_T_Terminal=$($ODB -u ''$V_BIT_N_ORG_USR'' -p ''$V_BIT_X_ORG_USR'' -d $BITACORA_DSN -x  "UPDATE $V_BIT_OWNER.TBL_HCH_RESULTADOS_LOBD_2 SET  FECHA_FIN='$FECHA_FIN',RESULTADO='$RESULTADO' WHERE PID='$PID_PADRE' and INC=$INC" 2>&1)
            echo FECHA_FIN $FECHA_FIN
            exit 1                                
        fi
        UPDATE_ESTATUS=$($ODB -u ''$V_BIT_N_ORG_USR'' -p ''$V_BIT_X_ORG_USR'' -d $BITACORA_DSN -x  "UPDATE $V_BIT_OWNER.TBL_HCH_RESULTADOS_LOBD_2 SET TODOS_ORIGEN=$TODOS_ORIGEN, PARCIAL_ORIGEN=$PARCIAL_ORIGEN, TODOS_DESTINO_A=$TODOS_DESTINO_A, TODOS_DESTINO_AA=$TODOS_DESTINO_AA, TODOS_STG=$TODOS_STG, TODOS_DESTINO=$TODOS_DESTINO, PARCIAL_DESTINO=$PARCIAL_DESTINO WHERE PID='$PID_PADRE' and INC=$INC" 2>&1)
    fi
#}>>$RUTA_LOG 2>&1

#principal

#exit 0

