use fin_onesumx;

invalidate metadata fin_fct_osx_segmental_cust_agg;
alter table fin_fct_osx_segmental_cust_agg drop partition (time_key=${var:tk});
alter table fin_fct_osx_segmental_cust_agg recover partitions;
compute incremental stats fin_fct_osx_segmental_cust_agg partition (time_key=${var:tk});

invalidate metadata fin_fct_osx_segmental_gl_agg;
alter table fin_fct_osx_segmental_gl_agg drop partition (time_key=${var:tk});
alter table fin_fct_osx_segmental_gl_agg recover partitions;
compute incremental stats fin_fct_osx_segmental_gl_agg partition (time_key=${var:tk});
