use fin_onesumx;

invalidate metadata fin_fct_osx_segmental_agg;
refresh fin_fct_osx_segmental_agg;
alter table fin_fct_osx_segmental_agg drop partition (time_key=${var:tk});
alter table fin_fct_osx_segmental_agg recover partitions;
compute incremental stats fin_fct_osx_segmental_agg partition (time_key=${var:tk});

invalidate metadata fin_fct_osx_trial_balance;
refresh fin_fct_osx_trial_balance;
alter table fin_fct_osx_trial_balance drop partition (time_key=${var:tk});
alter table fin_fct_osx_trial_balance recover partitions;
compute incremental stats fin_fct_osx_trial_balance partition (time_key=${var:tk});
