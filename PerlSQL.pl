#!/usr/bin/env perl64
################################################################################
#
# ����: ��perl�����Զ���SQL�ű���ȫ���ɹ�����0�������0
#
# �÷�: PerlSQL �Զ���SQL�ű� ETL����
#       �Զ���SQL�ο�example.sql
#
# ��ʷ: 20131019 yzq ���߰汾
#       20140311 yzq ���Ӷ�GP���ݿ��֧��
#       20140312 yzq ����HOME��������,����SHELL����֧��
#       20140315 yzq EXECEPTION_RAISE_OFFʱÿ����һ��SQL���ɹ�ʱ�Զ��ύCOMMIT��ʧ���Զ��ύROLLBACK;
#       20140429 yzq ����ZIPPER_TABLE����
#       20140506 yzq ����MERGE_TABLE����
#       20140513 yzq ֧�����÷������б༭����,���ɸ�ʽ
#       20140514 yzq ֧���Զ��������SELECT INTO��ֵ
#       20140515 yzq ȡ�����÷��������÷�ʽ,�ĳɴ� "���� => ����ֵ, ..." ���
#       20140515 yzq �������ں���������������Ч���б�
#       20140516 yzq ����SQL_FILE�����ñ���
#       20140519 yzq �޸�BUG
#       20140522 yzq ����$ENV::HOME�ȸ�ʽ�Ļ�����������
#       20140603 yzq Ĭ�������ʽ�ύcommit
#       20140607 yzq ֧��PRINTLOG�﷨:��C���Ը�ʽ ,�����־����ǰ������־�ļ�
#       20140611 yzq �޸�BUG
#       20140612 yzq �������ִ����Ϻ��ȡ��¼�������ñ���$SQLROWCNT
#       20140613 yzq ����SHELL��������Ĳ�׽
#       20140619 yzq ����MERGE����׼ȷ���ɱ䶯�����ֶι���
#       20140619 yzq �޸�BUG
#       20140625 yzq �޸�BUG
#       20140709 yzq SET��ֵ������NOT FOUND���б�����
#       20140718 yzq ��д��log_info��־��ӡ����,�ı�ԭ��ִ����ָ���ٴ�ӡ����ĸ��
#       20140721 yzq ������־��صĻ�������,������SHELL����ر�����ͻ
#       20140805 yzq �޸�BUG
#       20140820 yzq ���ӿսű�������ƣ���ֹ����ű����³ɹ��˳��Ĳ��
################################################################################


use strict;
use File::Basename;
use DBI;
use Public;
use HTTP::Date;

our $HOME			=	$ENV{HOME};
our $BUSIDB_TYPE;
our $BUSIDB_USER;
our $BUSIDB_NAME;
our $dbh;
our $ETL_DATE;
our $LAST_ETL_DATE;
our $SQL_FILE="";
our $SQL_LOG_FILE = $ENV{SQL_LOG_FILE} || ($HOME."/log/PerlSQL.log");
our $LINE_NO=0;
our $EXECEPTION_RAISE=1;
our $MAXDATE		=	"99991231";
our $MINDATE		=	"18991231";
our $MINTIME		=	"00:00:00";
our $FD_SQLFILE;
our %macro_var;
our %macro_local;
our $DEBUG_MODE;
our $GLOBAL_SEQ=0;
our $GLOBAL_VALID_SQL=0;

sub log_info
{
	my ($level,$file,$line,$msg) = @_;
	
	$file=basename $file;

	my $env_level=$ENV{SQL_LOG_LEVEL} || 0;

	#����ΪERROR(ֻ��ERROR��),��־Ϊ��E,ֱ�ӷ���
	return if ($env_level==2 and $level ne "E");

	#����ΪINFO(����DEBUG��),��־ΪD,ֱ�ӷ���
	return if ($env_level==1 and $level eq "D" );

	my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) =localtime(time);
	my $time=sprintf("%04d%02d%02d %02d:%02d:%02d",$year+1900,$mon+1,$mday,$hour,$min,$sec);

	my $LINE_DELIM="\n\n";
	$LINE_DELIM = "\n" if ( $level eq "D" );

	#��ӡ��Ϣ => ��Ļ���
	my $out_msg = "$level $time P[$$] F[$SQL_FILE] L[$LINE_NO]: $msg".$LINE_DELIM;
	print $out_msg;

	#��ӡ��Ϣ => ��־���
	if ( open( SQL_LOG_FILE,">>$SQL_LOG_FILE") ) {
		print SQL_LOG_FILE $out_msg;
		close(SQL_LOG_FILE);
	}

	#�ж�ERROR�˳�
	if ($level eq "E"){
		if ($dbh){
			$dbh->rollback if ( $dbh->{AutoCommit} == 0 );
			$dbh->disconnect;
		}
		close($FD_SQLFILE) if ( $FD_SQLFILE );
		die("ERROR:$line\n");
	}
}

sub db_conn
{
	my $BUSIDB_CONN = $ENV{BUSIDB_CONN};

	unless($BUSIDB_CONN){
		log_info "E",__FILE__,__LINE__, "�����û�������: BUSIDB_CONN";
	}

	($dbh,$BUSIDB_TYPE,$BUSIDB_USER,$BUSIDB_NAME) = Public->logon_by_file($BUSIDB_CONN);

	if ($dbh) {
		log_info "I",__FILE__,__LINE__,"�������ݿ�$BUSIDB_USER/****\@$BUSIDB_NAME�ɹ�";
	}
	else {
		log_info "E",__FILE__,__LINE__,"�������ݿ�$BUSIDB_USER/****\@$BUSIDB_NAMEʧ�� [".trim($DBI::errstr)."]";
	}
}

sub db_disc
{
	if ($dbh){
		$dbh->commit if ( $dbh->{AutoCommit} == 0 );
		$dbh->disconnect;
		log_info "I",__FILE__,__LINE__,"�ر����ݿ�ɹ�";
	}
}

sub sql_into
{
	my ($sql) = @_;
	$sql =~ s/\0|^\s+|\s+$|\;\s*$//g;

	return undef if (length($sql) == 0 );

	my @rows = $dbh->selectrow_array($sql);
	unless ( @rows ){
		if ( $DBI::errstr ) {
			if ($EXECEPTION_RAISE==1){
				log_info "E",__FILE__,__LINE__,"ִ��ʧ��: [".trim($DBI::errstr)."]";
			}else{
				log_info "I",__FILE__,__LINE__,"ִ�о���: [".trim($DBI::errstr)."]";
			}
		}else{
			log_info "E",__FILE__,__LINE__,"SQL NOT FOUND: û�н�����";
		}
		return undef;
	}

	return \@rows;
}

sub sql_exec
{
	my ($sql) = @_;
	$sql =~ s/\0|^\s+|\s+$|\;\s*$//g;

	#����SQLִ�к���Ч��¼��
	$macro_local{SQLROWCNT} = 0;

	return if (length($sql) == 0 );

	$GLOBAL_VALID_SQL = 1;
	log_info "D",__FILE__,__LINE__,"SQL: ".$sql;
	my $rows = $dbh->do($sql);
	unless($rows){
		if ($EXECEPTION_RAISE==1){
			log_info "E",__FILE__,__LINE__,"ִ��ʧ��: [".trim($DBI::errstr)."]";
		}else{
			log_info "I",__FILE__,__LINE__,"ִ�о���: [".trim($DBI::errstr)."]";
		}
	}else{
		unless ( $rows eq "0E0" ){
			#����SQLִ�к���Ч��¼��
			$macro_local{SQLROWCNT} = int($rows);
			log_info "I",__FILE__,__LINE__,"ִ�гɹ�: [��Ч��¼��$rows]";
		}else{
			log_info "I",__FILE__,__LINE__,"ִ�гɹ�";
		}
	}
}

#sub drop_temp_table
#{
#	my ( $table,$type ) = @_;
#	if ( grep /pg_temp.*\.$table$/i, $dbh->tables('','','',"TABLE") ){
#		sql_exec(qq{
#		DROP TABLE $table;
#		});
#	}
#}

sub test
{
	db_conn;
	sql_exec("      create table mytest (         aa varchar2(8) not null,        bb number,      cc varchar2(40),        primary key(aa) )");
#	sql_exec("\n\n\nupdate yzq set cycle = '3' where job_id='JOB_01'");
#	sql_exec("update yzq set cycle = '4' where job_id='JOB_02'");
#	sql_exec("rollback");
	db_disc;
}

#���ں���(date:yyyymmdd flag:D-�� M-�� term:�������)
sub date_fun
{
	my ( $date,$flag,$term ) = @_;

	return undef unless ( $date =~ /^\d{8}$/ );

	my $n = str2time($date);
	return undef unless ( $n );

	if ( $flag eq "D" )
	{
		my ($mday,$mon,$year,$wday) = (localtime( $n + 86400 * $term))[3,4,5,6];
		return sprintf("%04d%02d%02d",$year+1900,$mon+1,$mday);
	}
	elsif ( $flag eq "M" )
	{
		my ($mday,$mon,$year,$wday) = (localtime($n))[3,4,5,6];
		$mon += $term;
		$year += $mon / 12;
		$mon = $mon % 12;
		$mday = 31;
		for(0..4)
		{
			my $date_str = sprintf("%04d%02d%02d",$year+1900,$mon+1,$mday-$_);
			return $date_str if ( str2time($date_str) );
		};
	}
	return undef;
}



#��ӡ��־
sub printlog
{
	my ($line) = @_;
	my $info;
	my $stmt = "\$info = sprintf(".$line.")";

	unless ( eval($stmt) ) {
		log_info "I",__FILE__,__LINE__,$stmt;
		return 1;
	}

	log_info "I",__FILE__,__LINE__,$info;
	return 0;	
}

#�����Զ��庯������,���ɹ�ϣ����
sub process_paras
{
	my ($line) = @_;
	$line =~ s/\s//g;

	my %map;
	return undef unless ( eval( "%map = (".$line.")" ) );

	while (my ($key, $value) = each(%map))
	{
		delete $map{$key};
		$map{uc(trim($key))} = uc(trim($value));
	}

	return \%map;
}

sub MERGE_TABLE
{
	my ($line) = @_;

	my $t = process_paras($line);
	unless ($t){
		log_info "E",__FILE__,__LINE__,"�����Ƿ�,����[$line]";
		return 1;
	}
	my %map = %$t;

	MERGE_TABLE_BASE(\%map);
}

sub get_sequence
{
	$GLOBAL_SEQ = $GLOBAL_SEQ + 1;
	return $GLOBAL_SEQ;
}

sub get_defstr
{
	my ($col_type) = @_;
	my $defstr = "''";

	if ( $col_type =~ /INT|NUMERIC|DECIMAL/ ){
		$defstr = "0";
	}elsif ( $col_type =~ /^DATE$/ ) {
		$defstr = sprintf("CAST( '%s' AS DATE )",$MINDATE);
	}elsif ( $col_type =~ /^TIME$/ ) {
		$defstr = sprintf("CAST( '%s' AS TIME )",$MINTIME);
	}elsif ( $col_type =~ /^TIMESTAMP$/ ) {
		$defstr = sprintf("CAST( '%s %s' AS TIMESTAMP )",$MINDATE,$MINTIME);
	}else{
		$defstr = "''";
	}
	return $defstr;
}

sub MERGE_TABLE_BASE
{
	my ($tt) = @_;
	my %map = %$tt;

	for ( qw{ TARGET_TABLE SOURCE_TABLE KEY_COLS } ){
		unless($map{$_}){
			log_info "E",__FILE__,__LINE__,"����".$_."����Ϊ��,����";
			return 1;
		}
	}

	my (@select,@on,@where);

	my $sql = "SELECT * FROM ".$map{TARGET_TABLE}." WHERE 1<>1";
	my ($desc,$msg) = Public->sql_desc($dbh,$sql);
	unless ( defined($desc) ) {
		log_info "D",__FILE__,__LINE__,"SQL: ".$sql;
		log_info "E",__FILE__,__LINE__,$msg
	}
	for (@$desc)
	{
		my $col = $_->[0];
		push( @select,"o.".$col);
	}
	for ( split(",",$map{KEY_COLS}) )
	{
		push( @on, sprintf("n.%s = o.%s",$_,$_ ) );
		push( @where, sprintf("n.%s IS NULL",$_) );
	}

	my $select_str = join("\n\t\t\t,",@select);
	my $on_str = join("\n\t\t\tAnd ",@on);
	my $where_str = join("\n\t\t\tAnd ",@where);


	
	#�б䶯�����ֶ�,��Ҫ��ȷ�϶����䶯����������
	if ( defined($map{MODIFY_DT_COL}) ) {

		#ͨ���Ƚϻ��ʵ�ʱ䶯������
		my (@select_modify,@on_modify,@where_modify);
		for (@$desc)
		{
			my $col = $_->[0];
			my $col_type = $_->[1];
			my $defstr = get_defstr($col_type);

			if ( $col eq $map{MODIFY_DT_COL} ){
				push( @select_modify,sprintf("CAST( '%s' AS DATE ) AS %s",$ETL_DATE,$map{MODIFY_DT_COL}) );;
			}else{
				push( @select_modify,"n.".$col);

				my @keys = split(",",$map{KEY_COLS});
				if ( grep(/^$col$/,@keys) ) {
					push( @on_modify, sprintf("n.%s = o.%s",$col,$col) );
				}else{
					push( @on_modify, sprintf("COALESCE(n.%s,%s) = COALESCE(o.%s,%s)",$col,$defstr,$col,$defstr ) );
				}
			}
		}

		for ( split(",",$map{KEY_COLS}) )
		{
			push( @where_modify, sprintf("o.%s IS NULL",$_) );
		}
		my $select_modify_str = join("\n\t\t\t,",@select_modify);
		my $on_modify_str = join("\n\t\t\tAnd ",@on_modify);
		my $where_modify_str = join("\n\t\t\tAnd ",@where_modify);


		#������ʱ��
		my $table = (split(/\./,$map{TARGET_TABLE}))[-1];
		my $table_new = "YZQN".get_sequence()."_".$table;
		sql_exec(qq{
		CREATE GLOBAL TEMPORARY TABLE $table_new ( LIKE $map{TARGET_TABLE} ) ON COMMIT PRESERVE ROWS;
		});

		#����ʵ�ʱ䶯������������
		sql_exec(qq{

		INSERT INTO $table_new
		SELECT $select_modify_str
		  FROM $map{SOURCE_TABLE} n
		  LEFT OUTER JOIN $map{TARGET_TABLE} o
		    ON $on_modify_str
		 WHERE $where_modify_str;

		});

		#��ʵ�ʱ䶯����Ϊ����������ʹ��
		$map{SOURCE_TABLE} = $table_new;
	}


	if ( $map{MODE} eq "DEL_INS" ) {

		#ɾ�������ڽ����д��ڵ�����
		sql_exec(qq{
		DELETE FROM $map{TARGET_TABLE} o
			WHERE EXISTS(
			SELECT 1 FROM $map{SOURCE_TABLE} n WHERE
			     $on_str );
		});


	}else{

		#������ʱ��
		my $table = (split(/\./,$map{TARGET_TABLE}))[-1];
		my $table_roll = "YZQR".get_sequence()."_".$table;
		sql_exec(qq{
		CREATE GLOBAL TEMPORARY TABLE $table_roll ( LIKE $map{TARGET_TABLE} ) ON COMMIT PRESERVE ROWS;
		});

		#��ʼ���������ڽ����в����ڵĴ�������
		sql_exec(qq{

		INSERT INTO $table_roll
		SELECT $select_str
		  FROM $map{TARGET_TABLE} o
		  LEFT OUTER JOIN $map{SOURCE_TABLE} n
		    ON $on_str
		 WHERE $where_str;

		});

		#���Ŀ���
		sql_exec(qq{
		TRUNCATE TABLE $map{TARGET_TABLE};
		});

		#װ�������ڽ����в����ڵĴ�������
		sql_exec(qq{
		INSERT INTO $map{TARGET_TABLE}
		SELECT $select_str
		  FROM $table_roll o;
		});


	}

	#װ���������
	sql_exec(qq{
		INSERT INTO $map{TARGET_TABLE}
		SELECT $select_str
		  FROM $map{SOURCE_TABLE} o;
	});

	
	return 0;
}

sub ZIPPER_TABLE
{
	my ($line) = @_;

	my $t = process_paras($line);
	unless ($t){
		log_info "E",__FILE__,__LINE__,"�����Ƿ�,����[$line]";
		return 1;
	}
	my %map = %$t;
	$map{DATE} = $ETL_DATE unless ( $map{DATE} );

	ZIPPER_TABLE_BASE(\%map);
}

sub ZIPPER_TABLE_BASE
{
	my ($tt) = @_;
	my %map = %$tt;

	my @paras_not_null = qw{ TARGET_TABLE BGN_DT_COL END_DT_COL SOURCE_TABLE KEY_COLS DATE };
	for ( @paras_not_null ){
		unless($map{$_}){
			log_info "E",__FILE__,__LINE__,"����".$_."����Ϊ��,����";
			return 1;
		}
	}

	my ($cast_etl_date,$cast_max_date,$cast_min_date,$cast_min_time,$cast_min_timestamp) = (
		sprintf("CAST( '%s' AS DATE )",$map{DATE}),
		sprintf("CAST( '%s' AS DATE )",$MAXDATE),
		sprintf("CAST( '%s' AS DATE )",$MINDATE),
		sprintf("CAST( '%s' AS TIME )",$MINTIME),
		sprintf("CAST( '%s %s' AS TIMESTAMP )",$MINDATE,$MINTIME)
	);

	my %cols_type;
	my (@roll_select);
	my (@add_select,@add_on,@add_where);
	my (@TI_old_select,@TI_old_on);
	my (@TI_FULL_old_select,@TI_FULL_old_on);
	my (@DI_old_select,@DI_old_on,@DI_old_where);
	my (@DI_FULL_old_select,@DI_FULL_old_on,@DI_FULL_old_where);


	my $sql = "SELECT * FROM ".$map{TARGET_TABLE}." WHERE 1<>1";
	my ($desc,$msg) = Public->sql_desc($dbh,$sql);
	unless ( defined($desc) ) {
		log_info "D",__FILE__,__LINE__,"SQL: ".$sql;
		log_info "E",__FILE__,__LINE__,$msg
	}

	#����NODE_COLS���*ʱ
	if ( $map{NODE_COLS} eq "*" ){
		my @nodes=();
		my @keys = split(",",$map{KEY_COLS});
		for (@$desc){
			my $col = $_->[0];
			next if ( grep(/^$col$/,@keys) );
			next if ( $col eq $map{END_DT_COL} );
			next if ( $col eq $map{BGN_DT_COL} );
			push(@nodes,$col);
		}
		$map{NODE_COLS} = join(",",@nodes);
	}

	for (@$desc){
		my $col = $_->[0];
		$cols_type{$col} = $_->[1];

		#����roll��ѯ����
		if ( $col eq $map{END_DT_COL} ) {
			push( @roll_select,$cast_max_date );
		}else{
			push(@roll_select,$col);
		}

		#����add_select����
		if ( $col eq $map{BGN_DT_COL} ) {
			push( @add_select,$cast_etl_date );
		}elsif ( $col eq $map{END_DT_COL} ) {
			push( @add_select, $cast_max_date );
		}else{
			push( @add_select,"n.".$col);
		}

		#����TI_old_select����
		if ( $col eq $map{END_DT_COL} ) {
			my @tt;
			for ( split(",",$map{KEY_COLS}) ){
				push( @tt,sprintf("n.%s IS NOT NULL",$_) );
			}
			push( @TI_old_select,sprintf("CASE
				WHEN %s
				 AND n.%s IS NOT NULL
				 AND o.%s = %s THEN %s
				ELSE o.%s
			  END",
					join("\n\t\t\t\t AND ",@tt),
					$map{BGN_DT_COL},
					$map{END_DT_COL},
					$cast_max_date,
					$cast_etl_date,
					$map{END_DT_COL})
			);
		}else{
			push( @TI_old_select,"o.".$col);
		}

		#����DI_old_select����
		if ( $col eq $map{END_DT_COL} ) {
			push( @DI_old_select,$cast_etl_date );
		}else{
			push( @DI_old_select,"o.".$col );
		}

		#����TI_FULL_old_select����
		if ( $col eq $map{END_DT_COL} ) {
			my @tt;
			for ( split(",",$map{KEY_COLS}) ) {
				push( @tt,sprintf("n.%s IS NULL",$_) );
			}
			push( @TI_FULL_old_select,sprintf("CASE
				WHEN %s
				 AND o.%s = %s THEN %s
				ELSE o.%s
			  END",
					join("\n\t\t\t\t AND ",@tt),
					$map{END_DT_COL},
					$cast_max_date,
					$cast_etl_date,
					$map{END_DT_COL})
			);
		}else{
			push( @TI_FULL_old_select,"o.".$col);
		}

		#����DI_FULL_old_select����
		if ( $col eq $map{END_DT_COL} ) {
			push( @DI_FULL_old_select,$cast_etl_date );
		}else{
			push( @DI_FULL_old_select,"o.".$col );
		}
	}

	#����add_on����
	for ( split(",",$map{KEY_COLS}) ){
		push( @add_on, sprintf("n.%s = o.%s",$_,$_ ) );
	}
	for ( split(",",$map{NODE_COLS}) ){
		my $defstr = get_defstr($cols_type{$_});
		push( @add_on, sprintf("COALESCE(n.%s,%s) = COALESCE(o.%s,%s)",$_,$defstr,$_,$defstr ) );
	}
	push( @add_on, sprintf("o.%s = %s",$map{END_DT_COL},$cast_max_date) );



	#����add-where����
	for ( split(",",$map{KEY_COLS}) ){
		push( @add_where, sprintf("o.%s IS NULL",$_) );
	}
	push( @add_where, sprintf("o.%s IS NULL",$map{BGN_DT_COL}) );


	#����TI_old_on����
	for ( split(",",$map{KEY_COLS}) ){
		push( @TI_old_on, sprintf("n.%s = o.%s",$_,$_ ) );
	}

	#����DI_old_on����
	@DI_old_on = @TI_old_on;

	#����TI_FULL_old_on����
	@TI_FULL_old_on = @add_on;

	#����DI_FULL_old_on����
	@DI_FULL_old_on = @add_on;


	#����DI_old_where����
	my @tt1;
	for ( split(",",$map{KEY_COLS}) ){
		push( @tt1,sprintf("n.%s IS NOT NULL",$_) );
	}
	push( @DI_old_where,sprintf("%s
		 AND n.%s IS NOT NULL
		 AND o.%s = %s",
			join("\n\t\t AND ",@tt1),
			$map{BGN_DT_COL},
			$map{END_DT_COL},
			$cast_max_date)
	);

	#����DI_FULL_old_where����
	my @tt2;
	for ( split(",",$map{KEY_COLS}) ){
		push( @tt2,sprintf("n.%s IS NULL",$_) );
	}
	push( @DI_FULL_old_where,sprintf("%s
		 AND o.%s = %s",
			join("\n\t\t AND ",@tt2),
			$map{END_DT_COL},
			$cast_max_date)
	);

	my $roll_select_str = join("\n\t\t\t,",@roll_select);
	my $add_select_str = join("\n\t\t\t,",@add_select);
	my $add_on_str = join("\n\t\t\tAnd ",@add_on);
	my $add_where_str = join("\n\t\t\tAnd ",@add_where);
	my $TI_old_select_str = join("\n\t\t\t,",@TI_old_select);
	my $TI_FULL_old_select_str = join("\n\t\t\t,",@TI_FULL_old_select);
	my $TI_old_on_str = join("\n\t\t\tAnd ",@TI_old_on);
	my $TI_FULL_old_on_str = join("\n\t\t\tAnd ",@TI_FULL_old_on);
	my $DI_old_select_str = join("\n\t\t\t,",@DI_old_select);
	my $DI_FULL_old_select_str = join("\n\t\t\t,",@DI_FULL_old_select);
	my $DI_old_on_str = join("\n\t\t\tAnd ",@DI_old_on);
	my $DI_FULL_old_on_str = join("\n\t\t\tAnd ",@DI_FULL_old_on);
	my $DI_old_where_str = join("\n\t\t\tAnd ",@DI_old_where);
	my $DI_FULL_old_where_str = join("\n\t\t\tAnd ",@DI_FULL_old_where);


	#������ʱ��
	my $table = (split(/\./,$map{TARGET_TABLE}))[-1];
	my $table_new = "YZQN".get_sequence()."_".$table;
	my $table_old = "YZQO".get_sequence()."_".$table;
	my $table_roll= "YZQR".get_sequence()."_".$table;
	sql_exec(qq{
		CREATE GLOBAL TEMPORARY TABLE $table_new ( LIKE $map{TARGET_TABLE} ) ON COMMIT PRESERVE ROWS;
	});
	sql_exec(qq{
		CREATE GLOBAL TEMPORARY TABLE $table_old ( LIKE $map{TARGET_TABLE} ) ON COMMIT PRESERVE ROWS;
	});
	sql_exec(qq{
		CREATE GLOBAL TEMPORARY TABLE $table_roll ( LIKE $map{TARGET_TABLE} ) ON COMMIT PRESERVE ROWS;
	});
	


	#ɾ�������Ժ������
	sql_exec(qq{
		DELETE FROM $map{TARGET_TABLE}
		 WHERE $map{BGN_DT_COL} >= $cast_etl_date
		   AND $map{END_DT_COL} = $cast_max_date;
	});
	sql_exec(qq{
		DELETE FROM $map{TARGET_TABLE}
		 WHERE $map{BGN_DT_COL} >= $cast_etl_date
		   AND $map{END_DT_COL} <> $cast_max_date;
	});

	#���������������
	sql_exec(qq{
		INSERT INTO $table_roll
		SELECT $roll_select_str
		  FROM $map{TARGET_TABLE}
		 WHERE $map{END_DT_COL} >= $cast_etl_date
		   AND $map{END_DT_COL} <> $cast_max_date;
	});
	sql_exec(qq{
		DELETE FROM $map{TARGET_TABLE}
			WHERE $map{END_DT_COL} >= $cast_etl_date
			AND $map{END_DT_COL} <> $cast_max_date;
	});
	sql_exec(qq{
		INSERT INTO $map{TARGET_TABLE} SELECT * FROM $table_roll;
	});

	#ȡ�����������޸ĵ���Ϣ
	sql_exec(qq{
		INSERT INTO $table_new
		SELECT $add_select_str
		  FROM $map{SOURCE_TABLE} n
		  LEFT OUTER JOIN $map{TARGET_TABLE} o
			ON $add_on_str
		 WHERE $add_where_str;
	});


	#ȡ��û���仯Ҫ��������Ϣ
	if ( $map{SOURCE_FLAG} eq "FULL" ) {
		if ( $map{MODE} eq "DEL_INS" ) {
			sql_exec(qq{

		INSERT INTO $table_old
		SELECT $DI_FULL_old_select_str
		  FROM $map{TARGET_TABLE} o
		  LEFT OUTER JOIN $map{SOURCE_TABLE} n
			ON $DI_FULL_old_on_str
		 WHERE $DI_FULL_old_where_str;

			});
		}else{
			sql_exec(qq{

		INSERT INTO $table_old
		SELECT $TI_FULL_old_select_str
		  FROM $map{TARGET_TABLE} o
		  LEFT OUTER JOIN $map{SOURCE_TABLE} n
			ON $TI_FULL_old_on_str;

			});
		}
	}else{
		if ( $map{MODE} eq "DEL_INS" ) {
			sql_exec(qq{

		INSERT INTO $table_old
		SELECT $DI_old_select_str
		  FROM $map{TARGET_TABLE} o
		  LEFT OUTER JOIN $table_new n
			ON $DI_old_on_str
		 WHERE $DI_old_where_str;

			});
		}else{
			sql_exec(qq{

		INSERT INTO $table_old
		SELECT $TI_old_select_str
		  FROM $map{TARGET_TABLE} o
		  LEFT OUTER JOIN $table_new n
			ON $TI_old_on_str;

			});
		}
	}

	#��ʼ����Ŀ������
	if ( $map{MODE} eq "DEL_INS" ) {
		my @tt;
		for ( split(",",$map{KEY_COLS}) ){
			push( @tt, sprintf("n.%s = o.%s",$_,$_ ) );
		}
		my $tmp=join("\n\t\t\t AND ",@tt);
		sql_exec(qq{
		DELETE FROM $map{TARGET_TABLE} o
			WHERE o.$map{END_DT_COL} = $cast_max_date AND EXISTS(
			SELECT 1 FROM $table_old n WHERE
			     $tmp );
		});
	}else{
		sql_exec(qq{
			TRUNCATE TABLE $map{TARGET_TABLE};
		});
	}
	sql_exec(qq{
		INSERT INTO $map{TARGET_TABLE} SELECT * FROM $table_new;
	});
	sql_exec(qq{
		INSERT INTO $map{TARGET_TABLE} SELECT * FROM $table_old;
	});

	return 0;
}

sub SET
{
	my ($line) = @_;

	my ($vars,$sql) = split(/=/,$line,2);
	my @keys = split(/,/,$vars);
	if ( @keys <= 0 ){
		log_info "E",__FILE__,__LINE__,"δ�������";
		return 1;
	}

	my $t = sql_into($sql);
	my @values = ();
	@values = @$t if ($t);

	my $index = 0;
	for (my $index=0; $index<@keys; $index++) { 

		my $key = trim($keys[$index]);
		next if (length($key) <= 0);
		if(exists $macro_local{$key}){
			log_info "E",__FILE__,__LINE__,"�ñ���[$key]Ϊϵͳ����,��������ʹ��";
			return 1;
		}else{
			$macro_var{ $key } = $values[$index];
			log_info "I",__FILE__,__LINE__,"$key => $values[$index]";
		}

	}

	return 0;
}

#�û��Զ��幦�� TBD
sub user_define_process
{
	my ($line) = @_;
	$line =~ s/\0|^\s+|\s+$|\;\s*$//g;
	$line = trim($line);

	#�б��Ƿ�������쳣
	if     ( $line =~ /^EXECEPTION_RAISE_OFF$/i ){
		$EXECEPTION_RAISE=0;
		$dbh->{AutoCommit} = 1;
		log_info "I",__FILE__,__LINE__,"�����쳣�׳� - �Ժ�ÿ��SQLִ�гɹ����Զ��ύ����";
		return 0;
	}elsif ( $line =~ /^EXECEPTION_RAISE_ON$/i ){
		log_info "I",__FILE__,__LINE__,"���쳣�׳� - �Ժ�ÿ��SQL��Ҫ��ʽ�ύ����";
		$EXECEPTION_RAISE=1;
		$dbh->{AutoCommit} = 0;
		return 0;
	}elsif ( $line =~ /^COMMIT$/i ){
		if ( $dbh->commit ){
			log_info "I",__FILE__,__LINE__,"��ʽ�ύ����ɹ�";
			return 0;
		}else{
			log_info "E",__FILE__,__LINE__,"��ʽ�ύ����ʧ��";
			return 1;
		}
	}elsif ( $line =~ /^ROLLBACK$/i ){
		if ( $dbh->rollback ){
			log_info "I",__FILE__,__LINE__,"��ʽ�ع�����ɹ�";
			return 0;
		}else{
			log_info "E",__FILE__,__LINE__,"��ʽ�ع�����ʧ��";
			return 1;
		}
	}else{
		if ( $line =~ s/^SHELL\s*\(//i > 0 )
		{
			$line =~ s/\)$//i;
			log_info "D",__FILE__,__LINE__,"��ʼִ��SHELL����:[$line]";
			my $ret=system($line);
			$ret = $ret >>8;
			if ( $ret== 0 ){
				log_info "I",__FILE__,__LINE__,"ִ��SHELL�ɹ�";
				return 0;
			}else{
				log_info "E",__FILE__,__LINE__,"ִ��SHELL����ʧ��,������:[$ret] [$line]";
				return 1;
			}
		}elsif ( $line =~ s/^EXIT\s*\(//i > 0 ){
			$line =~ s/\)$//i;
			log_info "I",__FILE__,__LINE__,"����EXIT[$line]";
			if ($dbh){
				$dbh->rollback if ( $dbh->{AutoCommit} == 0 );
				$dbh->disconnect;
			}
			exit($line);
		}elsif ( $line =~ s/^ZIPPER_TABLE\s*\(//i > 0 ){
			$line =~ s/\)$//i;
			log_info "D",__FILE__,__LINE__,"��ʼִ�����ú���ZIPPER_TABLE:[$line]";
			if ( ZIPPER_TABLE($line) == 0 ){
				log_info "I",__FILE__,__LINE__,"ִ�����ú����ɹ�";
				return 0;
			}else{
				log_info "E",__FILE__,__LINE__,"ִ�����ú���ʧ��ZIPPER_TABLE:[$line]";
				return 1;
			}
		}elsif ( $line =~ s/^MERGE_TABLE\s*\(//i > 0 ){
			$line =~ s/\)$//i;
			log_info "D",__FILE__,__LINE__,"��ʼִ�����ú���MERGE_TABLE:[$line]";
			if ( MERGE_TABLE($line) == 0 ){
				log_info "I",__FILE__,__LINE__,"ִ�����ú����ɹ�";
				return 0;
			}else{
				log_info "E",__FILE__,__LINE__,"ִ�����ú���ʧ��MERGE_TABLE:[$line]";
				return 1;
			}
		}elsif ( $line =~ s/^SET\s+//i > 0 ){
			log_info "D",__FILE__,__LINE__,"��ʼִ�����ú���SET:[$line]";
			if ( SET($line) == 0 ){
				log_info "I",__FILE__,__LINE__,"ִ�����ú����ɹ�";
				return 0;
			}else{
				log_info "E",__FILE__,__LINE__,"ִ�����ú���ʧ��SET:[$line]";
				return 1;
			}
		}elsif ( $line =~ s/^PRINTLOG\s*\(//i > 0 ){
			$line =~ s/\)$//i;
			if ( printlog($line) == 0 ){
				return 0;
			}else{
				log_info "E",__FILE__,__LINE__,"ִ�����ú���ʧ��PRINTLOG:[$line]";
				return 1;
			}
		}
	}
	return 1;
}

sub trim
{
	my ($line) = @_;
	$line =~ s/\0|^\s+|\s+$//g;
	return $line;
}

#��ʼ���ڲ����� TBD
sub init_macro_local
{
	my $LAST_ETL_DATE = date_fun($ETL_DATE,"D",-1);

	$macro_local{MAXDATE} = $MAXDATE;
	$macro_local{MAXDATE} = $MAXDATE;
	$macro_local{MINDATE} = $MINDATE;
	$macro_local{HOME} = $HOME;
	$macro_local{BUSIDB_USER} = $BUSIDB_USER;
	$macro_local{BUSIDB_NAME} = $BUSIDB_NAME;
	$macro_local{ETL_DATE} = $ETL_DATE;
	$macro_local{LAST_ETL_DATE} = $LAST_ETL_DATE;
	$macro_local{SQL_FILE} = basename $SQL_FILE;
	$macro_local{SQL_LOG_FILE} = $SQL_LOG_FILE;
}

sub sql_line_process
{
	my ($line) = @_;

	chomp($line);

	#ȥ��"--"��׺����
	$line =~ s/--.*$//g;

	#ȥ�������� /* �� */������
	$line =~ s/\/\*.*\*\///g;

	#�滻��������
	$line =~ s/\$ENV::(\w+)/$ENV{$1}/g;

#   $line =~ s/\$(\w+)/$macro{uc($1)}/ge;           #�Ƿ������ᱻ�û��ɿ�

	#�滻�Զ������
	while (my ($key, $value) = each(%macro_var))    #�Ƿ����������û�
	{
		$line =~ s/\$$key/$value/g;
	}

	#�滻�ڲ�����
	while (my ($key, $value) = each(%macro_local))    #�Ƿ����������û�
	{
		$line =~ s/\$$key/$value/g;
	}

	return $line;
}

sub anykey
{
	my $anykey;

	printf(" ======= ����������� ======= (�˳�=q): ");
	chomp($anykey=<STDIN>);
	log_info "E",__FILE__,__LINE__,"�˹��˳�" if ( $anykey =~ /q/i );
}

sub process_sql
{
	my ($sql) = @_;

	anykey if ( $DEBUG_MODE =~ /^debug$/i );

	#�����Զ������
	if (user_define_process($sql)==0){
		$GLOBAL_VALID_SQL = 1;
		return;
	}

	#����û�б��滻�ı���ʱ����
	if ( $sql =~ /\$(\w+)/ ) {
		log_info "D",__FILE__,__LINE__,"SQL: ".$sql;
		log_info "E",__FILE__,__LINE__,"���ڷǷ�����[\$$1]"
	}

	#���Զ��������Ϊ��׼SQL���ִ��
	sql_exec($sql);
}

sub main
{
	#�������У��
	if (@ARGV < 2){
		die "Usage: PerlSQL sqlfile etldate [debug]!";
	}else{
		($SQL_FILE,$ETL_DATE,$DEBUG_MODE) = @ARGV;
	}

	#У������
	unless ( date_fun($ETL_DATE,'D',0) ){
		die "����[$ETL_DATE]�Ƿ�!";
	}

	#��sql�ű�
	unless(open($FD_SQLFILE,$SQL_FILE)){
		die "Can't open file $SQL_FILE!";
	}

	log_info "I",__FILE__,__LINE__," ======= �ű�:[$SQL_FILE] ETL����:[$ETL_DATE] =======";

	db_conn;

	init_macro_local;

	my $sql="";
	my $filter_flag=0;
	while ( <$FD_SQLFILE> ){
		my $line = $_;
		$LINE_NO = $LINE_NO + 1;

		#SQL�ű�Ԥ����,���˵���ע��--��/* ... */,�����Զ������
		$line = sql_line_process($line);

		#���˿���ע�� /* ... */
		if ( $filter_flag == 0 && $line =~ /\/\*/ ){
			$filter_flag = 1;
			next;
		}elsif ( $filter_flag == 1 && $line =~ /\*\// ){
			$filter_flag = 0;
			next;
		}
		next if ($filter_flag == 1);

		$sql = $sql."\n".$line;

		if ( $line =~ /\;\s*$/ ){
			process_sql($sql);
			$sql = "";
		}

	}

	process_sql($sql);

	log_info "E",__FILE__,__LINE__,"û��һ���Ϸ����,�����˳�" if ( $GLOBAL_VALID_SQL == 0 );

	close($FD_SQLFILE); 

	db_disc;
}

main
