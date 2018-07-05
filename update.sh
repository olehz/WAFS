#!/bin/bash
cd /home/oz/Dropbox/RR/WAFS/

NEW=$(printf "`date +%Y%m%d `%02d" $(( `date +%k -u`/6*6 )));OLD=$( cat last_update.log )
NEW='2017121012'

#if [ -f locked ] || [ "$OLD" == "$NEW" ] || ! wget --spider "ftp://ftp.ncep.noaa.gov/pub/data/nccf/com/gfs/prod/gfs.$NEW/WAFS_blended_${NEW}f06.grib2" 2>/dev/null; then
#    exit
#fi
date -u +%b%d[%H:%M:%S] > locked

declare -A FLS=( [800]=65 [700]=100 [600]=140 [500]=180 [400]=240 [350]=270 [300]=300 [250]=340 [200]=390 [150]=440 )
LAYERS=( "CAT 400 350 300 250 200 150" "ICIP 800 700 600 500 400 300" )
HRS=('06' '09' '12' '15' '18' '21' '24' '27' '30' '33' '36')
TBL='wafs';RAW='raw'

DIR='/home/oz/zzz/WAFS'
#DIR='/media/ramdisk';mkdir -p $DIR;mount -t tmpfs -o size=2048M tmpfs $DIR

PG_DB=vector;PG_USER=vector
psql -U$PG_USER -d$PG_DB -c "
DROP TABLE IF EXISTS import.$RAW CASCADE;
CREATE TABLE import.$RAW(
    layer varchar(4),
    fl smallint,
    hr smallint,
    dt timestamp without time zone,
    lv smallint,
    geom geometry(MULTIPOLYGON, 4326)
);"


mb2fl () {
    echo "(1-e(0.190284*l($1/1013.25)))*1453.6645" | bc -l
}

#rm $DIR/*
for HR in ${HRS[@]}
do
    if [[ `wget -S "ftp://ftp.ncep.noaa.gov/pub/data/nccf/com/gfs/prod/gfs.$NEW/WAFS_blended_${NEW}f${HR}.grib2" -O $DIR/$HR.grib2 2>&1 | grep '226 Transfer complete.'` ]]; then
        for i in ${!LAYERS[*]}
        do
            IFS=' ' read -r -a AR <<< ${LAYERS[$i]}
            LAYER=${AR[0]}
            MIN_FL=$(echo "$(mb2fl ${AR[1]}) / 20 * 20" | bc)
            MAX_FL=$(echo "$(mb2fl ${AR[-1]}) / 20 * 20" | bc)
            for (( FL=$MIN_FL; FL<=$MAX_FL; FL+=10 ));
            do
                if (( $FL == $MIN_FL )); then
                    MB=${AR[1]}
                else
                    MB=$(echo "(e(l(1-$FL/1453.6645)/0.190284)*1013.25)" | bc -l | awk '{printf("%d\n",$1 + 0.5)}')
                fi
                for (( k=2; k<${#AR[@]}; k++ ));
                do
                    MB1=${AR[$k-1]}
                    MB2=${AR[$k]}
                    if (( $MB1 >= $MB )) && (( $MB >= $MB2 )); then
                        if (( $FL == $MIN_FL )); then
                            wgrib2 $DIR/$HR.grib2 -match ":$LAYER:$MB mb:$(echo $HR | sed 's/^0*//') hour fcst:spatial max" -grib_out $DIR/filtered.grib2
                        else
                            SCL=$(echo "($MB-$MB2)/($MB1-$MB2)" | bc -l)
                            wgrib2 $DIR/$HR.grib2 \
                                -ncpu 4 \
                                -if ":$LAYER:$MB1 mb:$(echo $HR | sed 's/^0*//') hour fcst:spatial max" -rpn "sto_1" -fi \
                                -if ":$LAYER:$MB2 mb:$(echo $HR | sed 's/^0*//') hour fcst:spatial max" -rpn "sto_2" -fi \
                                -if_reg "1:2" \
                                -rpn "rcl_1:rcl_2:-:0$SCL:*:rcl_2:+" \
                                -set_var $LAYER -grib_out $DIR/filtered.grib2 \
                                -if_reg "1:2" -rpn "clr_1:clr_2" -fi
                        fi

                        rm $DIR/*.tif
                        gribdoctor smoosh -dev -uw $DIR/filtered.grib2 $DIR/center.tif
                        gdalwarp -s_srs '+proj=latlong +datum=WGS84 +pm=0dW' -t_srs '+proj=latlong +datum=WGS84 +pm=360dW' $DIR/center.tif $DIR/right.tif
                        gdalwarp -s_srs '+proj=latlong +datum=WGS84 +pm=0dW' -t_srs '+proj=latlong +datum=WGS84 +pm=360dE' $DIR/center.tif $DIR/left.tif
                        gdal_merge.py -o $DIR/merged.tif $DIR/left.tif $DIR/center.tif $DIR/right.tif
                        gdalwarp -te -190 -90 190 90 $DIR/merged.tif $DIR/unwrap.tif

                        makesurface vectorize -ov lv --classfile levels/$LAYER --smoothing 5 $DIR/unwrap.tif --outfile $DIR/poly.geojson
                        ogr2ogr -update -append -f PostgreSQL "PG:dbname=$PG_DB user=$PG_USER host=localhost port=5432" $DIR/poly.geojson -nln import.$RAW -where "lv>1"
                        psql -U$PG_USER -d$PG_DB -c "UPDATE import.$RAW SET
                            layer = '${LAYER,,}',
                            fl = $FL,
                            hr = $HR,
                            dt = to_timestamp('${NEW}', 'YYYYMMDDHH24') + interval '${HR} hour'
                        WHERE fl IS NULL";

                    fi
                done
            done
            #exit
            #for (( j=1; j<${#AR[@]}; j++ ));
            #do
                #MB=${AR[$j]}
                #FL=${FLS[$MB]}
                #wgrib2 $DIR/$HR.grib2 -match "$LAYER:$MB mb:$(echo $HR | sed 's/^0*//') hour fcst:spatial max" -grib_out $DIR/filtered.grib2
                #rm $DIR/*.tif
                #gribdoctor smoosh -dev -uw $DIR/filtered.grib2 $DIR/center.tif
                #gdalwarp -s_srs '+proj=latlong +datum=WGS84 +pm=0dW' -t_srs '+proj=latlong +datum=WGS84 +pm=360dW' $DIR/center.tif $DIR/right.tif
                #gdalwarp -s_srs '+proj=latlong +datum=WGS84 +pm=0dW' -t_srs '+proj=latlong +datum=WGS84 +pm=360dE' $DIR/center.tif $DIR/left.tif
                #gdal_merge.py -o $DIR/merged.tif $DIR/left.tif $DIR/center.tif $DIR/right.tif
                #gdalwarp -te -190 -90 190 90 $DIR/merged.tif $DIR/unwrap.tif

                #makesurface vectorize -ov lv --classfile levels/$LAYER --smoothing 5 $DIR/unwrap.tif --outfile $DIR/poly.geojson
                #ogr2ogr -update -append -f PostgreSQL "PG:dbname=$PG_DB user=$PG_USER host=localhost port=5432" $DIR/poly.geojson -nln import.$RAW -where "lv>1"
                #psql -U$PG_USER -d$PG_DB -c "UPDATE import.$RAW SET
                #    layer = '${LAYER,,}',
                #    fl = $FL,
                #    hr = to_timestamp('${NEW}', 'YYYYMMDDHH24') + interval '${HR} hour'
                #WHERE fl IS NULL";
            #done
        done
        rm $DIR/$HR.grib2
    fi
done

psql -U$PG_USER -d$PG_DB -c "
CREATE TABLE import.$TBL AS
SELECT ROW_NUMBER() OVER() id, layer, fl, hr, dt, lv-2 AS lv, ST_MakeValid( (ST_Dump(ST_CollectionExtract(geom, 3))).geom::geometry(POLYGON, 4326) ) AS geom
FROM (
    SELECT layer, fl, hr, dt, lv, ST_Intersection(poly, b.geom) AS geom
    FROM import.$RAW w,
    (SELECT ST_GeomFromText('POLYGON((-180 -89, -180 89, 180 89, 180 -89, -180 -89))', 4326) AS geom) b
    INNER JOIN LATERAL (SELECT ST_MakePolygon(poly[1], poly[2:]) poly FROM (
        SELECT ARRAY_AGG(ST_ExteriorRing(poly.geom)) poly FROM ST_Dump(w.geom) poly
    ) m) n ON TRUE
    WHERE ST_Intersects(poly, b.geom)
) z
WHERE ST_GeometryType(geom) IN ('ST_Polygon', 'ST_MultiPolygon', 'ST_GeometryCollection')
ORDER BY layer, fl, hr, dt, lv;
DROP TABLE IF EXISTS import.$RAW CASCADE"

if [ "$( psql -U$PG_USER -d$PG_DB -tAc "SELECT COUNT(to_regclass('public.$TBL'))" )" = '1' ]; then
    psql -U$PG_USER -d$PG_DB -c "INSERT INTO import.$TBL(layer, fl, hr, dt, lv, geom)
        SELECT layer, fl, hr, dt, lv, geom FROM public.$TBL o
        WHERE o.dt BETWEEN to_timestamp('${NEW}', 'YYYYMMDDHH24') - interval '6 hour'
        AND to_timestamp('${NEW}', 'YYYYMMDDHH24') + interval '${HRS[0]} hour';"
fi

psql -U$PG_USER -d$PG_DB -c "
CREATE INDEX ${TABLE}_geom_idx ON import.$TBL USING gist (geom);
CREATE INDEX ${TABLE}_geohash ON import.$TBL (ST_GeoHash(Box2D(geom)));
CLUSTER ${TABLE}_geohash ON import.$TBL;
ANALYZE import.$TBL;
DROP TABLE IF EXISTS public.$TBL CASCADE;
ALTER TABLE import.$TBL SET SCHEMA public;
"

echo $NEW > last_update.log
echo "$NEW `cat locked` -> `date -u +%b%d[%H:%M:%S]`" >> WAFS.log
rm locked
#umount $DIR
exit

psql -U$PG_USER -d$PG_DB -c "
UPDATE import.$TBL w SET zoom = z.zoom FROM (
    SELECT id, COALESCE((SELECT MAX(zoom) FROM UNNEST(ARRAY_AGG(z)) zoom WHERE ROUND(SQRT(ST_Area(ST_Transform(geom, 3857)))) < ZRes(zoom)), 0) AS zoom
    FROM import.$TBL, generate_series(0, 6) z
    GROUP BY id
) z WHERE z.id = w.id;
"
