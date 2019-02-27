#!/usr/bin/python2.7
#
# Assignment2 Interface
#

import psycopg2
import os
import sys
# Donot close the connection inside this file i.e. do not perform openconnection.close()
def RangeQuery(ratingsTableName, ratingMinValue, ratingMaxValue, openconnection):
    try:
        cursor = openconnection.cursor()
        totallist = []

        # RangeQuery - rangeratings tables
        cursor.execute("SELECT partitionnum FROM rangeratingsmetadata where " + str(
            ratingMinValue) + " BETWEEN minrating and maxrating")
        minlimittable = cursor.fetchone()[0]
        cursor.execute("SELECT partitionnum FROM rangeratingsmetadata where " + str(
            ratingMaxValue) + " BETWEEN minrating and maxrating")
        maxlimittable = cursor.fetchone()[0]

        for x in range(minlimittable, maxlimittable+1):
            totallist.append(
                "SELECT 'RangeRatingsPart" + str(
                    x) + "' AS tablename, userid, movieid, rating FROM rangeratingspart" + str(
                    x) +
                " where rating >=" + str(ratingMinValue) + " AND rating <=" + str(ratingMaxValue))

        # RangeQuery - roundrobinratings tables
        cursor.execute("SELECT partitionnum FROM roundrobinratingsmetadata")
        RRpartitons = cursor.fetchone()[0]
        for x in range(RRpartitons):
            totallist.append("SELECT 'RoundRobinRatingsPart" + str(
                x) + "' AS tablename, userid, movieid, rating FROM roundrobinratingspart" + str(x) +
                             " where rating >=" + str(ratingMinValue) + " AND rating <=" + str(ratingMaxValue))

        # combine both range and rr
        listquery = "SELECT * FROM ({0}) AS TotalList".format("UNION ALL ".join(totallist))

        loadin = open("RangeQueryOut", 'w')
        cursor.copy_expert('COPY (' + listquery + ') to STDOUT (DELIMITER ",")', loadin)

        loadin.close()
        openconnection.commit()

    except psycopg2.DatabaseError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    except IOError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)

    finally:
        if cursor:
            cursor.close()

def PointQuery(ratingsTableName, ratingValue, openconnection):
    try:
        cursor = openconnection.cursor()
        totallist = []

        # point query - range partition
        cursor.execute("SELECT partitionnum FROM rangeratingsmetadata where " + str(
            ratingValue) + " BETWEEN minrating and maxrating")
        rangepartitionum = cursor.fetchone()[0]
        totallist.append("SELECT 'RangeRatingsPart" + str(
            rangepartitionum) + "' AS tablename, userid, movieid, rating FROM rangeratingspart" + str(
            rangepartitionum) + " where rating =" + str(ratingValue))

        # point query - round robin partiton
        cursor.execute("SELECT partitionnum FROM roundrobinratingsmetadata")
        RRpartitons = cursor.fetchone()[0]
        for x in range(RRpartitons):
            totallist.append("SELECT 'RoundRobinRatingspart" + str(
                x) + "' AS tablename, userid, movieid, rating FROM roundrobinratingspart" + str(
                x) + " where rating =" + str(ratingValue))

        # combine both range and rr
        listquery = "SELECT * FROM ({0}) AS TotalList".format("UNION ALL ".join(totallist))

        loadin = open("PointQueryOut", 'w')
        cursor.copy_expert('COPY (' + listquery + ') to STDOUT (DELIMITER ",")', loadin)

        loadin.close()
        openconnection.commit()

    except psycopg2.DatabaseError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    except IOError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)

    finally:
        if cursor:
            cursor.close()