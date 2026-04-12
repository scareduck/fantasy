SET FOREIGN_KEY_CHECKS=0;

DROP TABLE IF EXISTS espn_forecaster_snapshot;
DROP TABLE IF EXISTS player_external_id;
DROP TABLE IF EXISTS roster_move;
DROP TABLE IF EXISTS stream_note;
DROP TABLE IF EXISTS projection;
DROP TABLE IF EXISTS probable_start;
DROP TABLE IF EXISTS player_availability_snapshot;
DROP TABLE IF EXISTS sync_run;
DROP TABLE IF EXISTS league_stat_category;
DROP TABLE IF EXISTS player;
DROP TABLE IF EXISTS league;

SET FOREIGN_KEY_CHECKS=1;

SOURCE schema.sql;
