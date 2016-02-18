#!/usr/bin/env perl64
################################################################################
#
# 描述: 用perl解析自定义SQL脚本，全部成功返回0，否则非0
#
# 用法: PerlSQL 自定义SQL脚本 ETL日期
#       自定义SQL参考example.sql
#
# 历史: 20131019 yzq 基线版本
#       20140311 yzq 增加对GP数据库的支持
#       20140312 yzq 增加HOME变量引入,增加SHELL运行支持
#       20140315 yzq EXECEPTION_RAISE_OFF时每运行一句SQL，成功时自动提交COMMIT，失败自动提交ROLLBACK;
#       20140429 yzq 增加ZIPPER_TABLE方法
#       20140506 yzq 增加MERGE_TABLE方法
#       20140513 yzq 支持内置方法跨行编辑调用,自由格式
#       20140514 yzq 支持自定义变量及SELECT INTO赋值
#       20140515 yzq 取消内置方法读配置方式,改成纯 "参数 => 参数值, ..." 风格
#       20140515 yzq 增加日期函数及输入日期有效性判别
#       20140516 yzq 增加SQL_FILE等内置变量
#       20140519 yzq 修复BUG
#       20140522 yzq 增加$ENV::HOME等格式的环境变量引入
#       20140603 yzq 默认最后隐式提交commit
#       20140607 yzq 支持PRINTLOG语法:仿C语言格式 ,输出日志到当前屏和日志文件
#       20140611 yzq 修复BUG
#       20140612 yzq 增加语句执行完毕后获取记录数的内置变量$SQLROWCNT
#       20140613 yzq 增加SHELL出错返回码的捕捉
#       20140619 yzq 增加MERGE函数准确生成变动日期字段功能
#       20140619 yzq 修复BUG
#       20140625 yzq 修复BUG
#       20140709 yzq SET赋值语句针对NOT FOUND进行报错处理
#       20140718 yzq 改写了log_info日志打印函数,改变原先执行完指令再打印输出的格局
#       20140721 yzq 调整日志相关的环境变量,避免与SHELL中相关变量冲突
#       20140805 yzq 修复BUG
#       20140820 yzq 增加空脚本报错机制，防止乱码脚本导致成功退出的差错
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

	#级别为ERROR(只打ERROR的),日志为非E,直接返回
	return if ($env_level==2 and $level ne "E");

	#级别为INFO(不打DEBUG的),日志为D,直接返回
	return if ($env_level==1 and $level eq "D" );

	my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) =localtime(time);
	my $time=sprintf("%04d%02d%02d %02d:%02d:%02d",$year+1900,$mon+1,$mday,$hour,$min,$sec);

	my $LINE_DELIM="\n\n";
	$LINE_DELIM = "\n" if ( $level eq "D" );

	#打印信息 => 屏幕输出
	my $out_msg = "$level $time P[$$] F[$SQL_FILE] L[$LINE_NO]: $msg".$LINE_DELIM;
	print $out_msg;

	#打印信息 => 日志输出
	if ( open( SQL_LOG_FILE,">>$SQL_LOG_FILE") ) {
		print SQL_LOG_FILE $out_msg;
		close(SQL_LOG_FILE);
	}

	#判断ERROR退出
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
		log_info "E",__FILE__,__LINE__, "请设置环境变量: BUSIDB_CONN";
	}

	($dbh,$BUSIDB_TYPE,$BUSIDB_USER,$BUSIDB_NAME) = Public->logon_by_file($BUSIDB_CONN);

	if ($dbh) {
		log_info "I",__FILE__,__LINE__,"连接数据库$BUSIDB_USER/****\@$BUSIDB_NAME成功";
	}
	else {
		log_info "E",__FILE__,__LINE__,"连接数据库$BUSIDB_USER/****\@$BUSIDB_NAME失败 [".trim($DBI::errstr)."]";
	}
}

sub db_disc
{
	if ($dbh){
		$dbh->commit if ( $dbh->{AutoCommit} == 0 );
		$dbh->disconnect;
		log_info "I",__FILE__,__LINE__,"关闭数据库成功";
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
				log_info "E",__FILE__,__LINE__,"执行失败: [".trim($DBI::errstr)."]";
			}else{
				log_info "I",__FILE__,__LINE__,"执行警告: [".trim($DBI::errstr)."]";
			}
		}else{
			log_info "E",__FILE__,__LINE__,"SQL NOT FOUND: 没有结果输出";
		}
		return undef;
	}

	return \@rows;
}

sub sql_exec
{
	my ($sql) = @_;
	$sql =~ s/\0|^\s+|\s+$|\;\s*$//g;

	#本次SQL执行后有效记录数
	$macro_local{SQLROWCNT} = 0;

	return if (length($sql) == 0 );

	$GLOBAL_VALID_SQL = 1;
	log_info "D",__FILE__,__LINE__,"SQL: ".$sql;
	my $rows = $dbh->do($sql);
	unless($rows){
		if ($EXECEPTION_RAISE==1){
			log_info "E",__FILE__,__LINE__,"执行失败: [".trim($DBI::errstr)."]";
		}else{
			log_info "I",__FILE__,__LINE__,"执行警告: [".trim($DBI::errstr)."]";
		}
	}else{
		unless ( $rows eq "0E0" ){
			#本次SQL执行后有效记录数
			$macro_local{SQLROWCNT} = int($rows);
			log_info "I",__FILE__,__LINE__,"执行成功: [有效记录数$rows]";
		}else{
			log_info "I",__FILE__,__LINE__,"执行成功";
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

#日期函数(date:yyyymmdd flag:D-日 M-月 term:间隔周期)
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



#打印日志
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

#处理自定义函数参数,生成哈希数组
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
		log_info "E",__FILE__,__LINE__,"参数非法,请检查[$line]";
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
			log_info "E",__FILE__,__LINE__,"参数".$_."不能为空,请检查";
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


	
	#有变动日期字段,需要精确认定出变动或新增日期
	if ( defined($map{MODIFY_DT_COL}) ) {

		#通过比较获得实际变动的数据
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


		#创建临时表
		my $table = (split(/\./,$map{TARGET_TABLE}))[-1];
		my $table_new = "YZQN".get_sequence()."_".$table;
		sql_exec(qq{
		CREATE GLOBAL TEMPORARY TABLE $table_new ( LIKE $map{TARGET_TABLE} ) ON COMMIT PRESERVE ROWS;
		});

		#生成实际变动或新增的数据
		sql_exec(qq{

		INSERT INTO $table_new
		SELECT $select_modify_str
		  FROM $map{SOURCE_TABLE} n
		  LEFT OUTER JOIN $map{TARGET_TABLE} o
		    ON $on_modify_str
		 WHERE $where_modify_str;

		});

		#将实际变动表作为今日增量表使用
		$map{SOURCE_TABLE} = $table_new;
	}


	if ( $map{MODE} eq "DEL_INS" ) {

		#删除昨日在今日中存在的数据
		sql_exec(qq{
		DELETE FROM $map{TARGET_TABLE} o
			WHERE EXISTS(
			SELECT 1 FROM $map{SOURCE_TABLE} n WHERE
			     $on_str );
		});


	}else{

		#创建临时表
		my $table = (split(/\./,$map{TARGET_TABLE}))[-1];
		my $table_roll = "YZQR".get_sequence()."_".$table;
		sql_exec(qq{
		CREATE GLOBAL TEMPORARY TABLE $table_roll ( LIKE $map{TARGET_TABLE} ) ON COMMIT PRESERVE ROWS;
		});

		#开始备份昨日在今日中不存在的存量数据
		sql_exec(qq{

		INSERT INTO $table_roll
		SELECT $select_str
		  FROM $map{TARGET_TABLE} o
		  LEFT OUTER JOIN $map{SOURCE_TABLE} n
		    ON $on_str
		 WHERE $where_str;

		});

		#清空目标表
		sql_exec(qq{
		TRUNCATE TABLE $map{TARGET_TABLE};
		});

		#装入昨日在今日中不存在的存量数据
		sql_exec(qq{
		INSERT INTO $map{TARGET_TABLE}
		SELECT $select_str
		  FROM $table_roll o;
		});


	}

	#装入今日数据
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
		log_info "E",__FILE__,__LINE__,"参数非法,请检查[$line]";
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
			log_info "E",__FILE__,__LINE__,"参数".$_."不能为空,请检查";
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

	#处理NODE_COLS配成*时
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

		#生成roll查询序列
		if ( $col eq $map{END_DT_COL} ) {
			push( @roll_select,$cast_max_date );
		}else{
			push(@roll_select,$col);
		}

		#生成add_select序列
		if ( $col eq $map{BGN_DT_COL} ) {
			push( @add_select,$cast_etl_date );
		}elsif ( $col eq $map{END_DT_COL} ) {
			push( @add_select, $cast_max_date );
		}else{
			push( @add_select,"n.".$col);
		}

		#生成TI_old_select序列
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

		#生成DI_old_select序列
		if ( $col eq $map{END_DT_COL} ) {
			push( @DI_old_select,$cast_etl_date );
		}else{
			push( @DI_old_select,"o.".$col );
		}

		#生成TI_FULL_old_select序列
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

		#生成DI_FULL_old_select序列
		if ( $col eq $map{END_DT_COL} ) {
			push( @DI_FULL_old_select,$cast_etl_date );
		}else{
			push( @DI_FULL_old_select,"o.".$col );
		}
	}

	#生成add_on序列
	for ( split(",",$map{KEY_COLS}) ){
		push( @add_on, sprintf("n.%s = o.%s",$_,$_ ) );
	}
	for ( split(",",$map{NODE_COLS}) ){
		my $defstr = get_defstr($cols_type{$_});
		push( @add_on, sprintf("COALESCE(n.%s,%s) = COALESCE(o.%s,%s)",$_,$defstr,$_,$defstr ) );
	}
	push( @add_on, sprintf("o.%s = %s",$map{END_DT_COL},$cast_max_date) );



	#生成add-where序列
	for ( split(",",$map{KEY_COLS}) ){
		push( @add_where, sprintf("o.%s IS NULL",$_) );
	}
	push( @add_where, sprintf("o.%s IS NULL",$map{BGN_DT_COL}) );


	#生成TI_old_on序列
	for ( split(",",$map{KEY_COLS}) ){
		push( @TI_old_on, sprintf("n.%s = o.%s",$_,$_ ) );
	}

	#生成DI_old_on序列
	@DI_old_on = @TI_old_on;

	#生成TI_FULL_old_on序列
	@TI_FULL_old_on = @add_on;

	#生成DI_FULL_old_on序列
	@DI_FULL_old_on = @add_on;


	#生成DI_old_where序列
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

	#生成DI_FULL_old_where序列
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


	#创建临时表
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
	


	#删除当天以后的新链
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

	#当天结束的链续回
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

	#取出新增或有修改的信息
	sql_exec(qq{
		INSERT INTO $table_new
		SELECT $add_select_str
		  FROM $map{SOURCE_TABLE} n
		  LEFT OUTER JOIN $map{TARGET_TABLE} o
			ON $add_on_str
		 WHERE $add_where_str;
	});


	#取出没变或变化要关链的信息
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

	#开始生成目标数据
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
		log_info "E",__FILE__,__LINE__,"未定义变量";
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
			log_info "E",__FILE__,__LINE__,"该变量[$key]为系统变量,不允许定义使用";
			return 1;
		}else{
			$macro_var{ $key } = $values[$index];
			log_info "I",__FILE__,__LINE__,"$key => $values[$index]";
		}

	}

	return 0;
}

#用户自定义功能 TBD
sub user_define_process
{
	my ($line) = @_;
	$line =~ s/\0|^\s+|\s+$|\;\s*$//g;
	$line = trim($line);

	#判别是否出错抛异常
	if     ( $line =~ /^EXECEPTION_RAISE_OFF$/i ){
		$EXECEPTION_RAISE=0;
		$dbh->{AutoCommit} = 1;
		log_info "I",__FILE__,__LINE__,"屏蔽异常抛出 - 以后每句SQL执行成功将自动提交事务";
		return 0;
	}elsif ( $line =~ /^EXECEPTION_RAISE_ON$/i ){
		log_info "I",__FILE__,__LINE__,"打开异常抛出 - 以后每句SQL需要显式提交事务";
		$EXECEPTION_RAISE=1;
		$dbh->{AutoCommit} = 0;
		return 0;
	}elsif ( $line =~ /^COMMIT$/i ){
		if ( $dbh->commit ){
			log_info "I",__FILE__,__LINE__,"显式提交事务成功";
			return 0;
		}else{
			log_info "E",__FILE__,__LINE__,"显式提交事务失败";
			return 1;
		}
	}elsif ( $line =~ /^ROLLBACK$/i ){
		if ( $dbh->rollback ){
			log_info "I",__FILE__,__LINE__,"显式回滚事务成功";
			return 0;
		}else{
			log_info "E",__FILE__,__LINE__,"显式回滚事务失败";
			return 1;
		}
	}else{
		if ( $line =~ s/^SHELL\s*\(//i > 0 )
		{
			$line =~ s/\)$//i;
			log_info "D",__FILE__,__LINE__,"开始执行SHELL命令:[$line]";
			my $ret=system($line);
			$ret = $ret >>8;
			if ( $ret== 0 ){
				log_info "I",__FILE__,__LINE__,"执行SHELL成功";
				return 0;
			}else{
				log_info "E",__FILE__,__LINE__,"执行SHELL命令失败,返回码:[$ret] [$line]";
				return 1;
			}
		}elsif ( $line =~ s/^EXIT\s*\(//i > 0 ){
			$line =~ s/\)$//i;
			log_info "I",__FILE__,__LINE__,"程序EXIT[$line]";
			if ($dbh){
				$dbh->rollback if ( $dbh->{AutoCommit} == 0 );
				$dbh->disconnect;
			}
			exit($line);
		}elsif ( $line =~ s/^ZIPPER_TABLE\s*\(//i > 0 ){
			$line =~ s/\)$//i;
			log_info "D",__FILE__,__LINE__,"开始执行内置函数ZIPPER_TABLE:[$line]";
			if ( ZIPPER_TABLE($line) == 0 ){
				log_info "I",__FILE__,__LINE__,"执行内置函数成功";
				return 0;
			}else{
				log_info "E",__FILE__,__LINE__,"执行内置函数失败ZIPPER_TABLE:[$line]";
				return 1;
			}
		}elsif ( $line =~ s/^MERGE_TABLE\s*\(//i > 0 ){
			$line =~ s/\)$//i;
			log_info "D",__FILE__,__LINE__,"开始执行内置函数MERGE_TABLE:[$line]";
			if ( MERGE_TABLE($line) == 0 ){
				log_info "I",__FILE__,__LINE__,"执行内置函数成功";
				return 0;
			}else{
				log_info "E",__FILE__,__LINE__,"执行内置函数失败MERGE_TABLE:[$line]";
				return 1;
			}
		}elsif ( $line =~ s/^SET\s+//i > 0 ){
			log_info "D",__FILE__,__LINE__,"开始执行内置函数SET:[$line]";
			if ( SET($line) == 0 ){
				log_info "I",__FILE__,__LINE__,"执行内置函数成功";
				return 0;
			}else{
				log_info "E",__FILE__,__LINE__,"执行内置函数失败SET:[$line]";
				return 1;
			}
		}elsif ( $line =~ s/^PRINTLOG\s*\(//i > 0 ){
			$line =~ s/\)$//i;
			if ( printlog($line) == 0 ){
				return 0;
			}else{
				log_info "E",__FILE__,__LINE__,"执行内置函数失败PRINTLOG:[$line]";
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

#初始化内部变量 TBD
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

	#去除"--"后缀内容
	$line =~ s/--.*$//g;

	#去除单行中 /* 和 */的内容
	$line =~ s/\/\*.*\*\///g;

	#替换环境变量
	$line =~ s/\$ENV::(\w+)/$ENV{$1}/g;

#   $line =~ s/\$(\w+)/$macro{uc($1)}/ge;           #非法变量会被置换成空

	#替换自定义变量
	while (my ($key, $value) = each(%macro_var))    #非法变量不作置换
	{
		$line =~ s/\$$key/$value/g;
	}

	#替换内部变量
	while (my ($key, $value) = each(%macro_local))    #非法变量不作置换
	{
		$line =~ s/\$$key/$value/g;
	}

	return $line;
}

sub anykey
{
	my $anykey;

	printf(" ======= 按任意键继续 ======= (退出=q): ");
	chomp($anykey=<STDIN>);
	log_info "E",__FILE__,__LINE__,"人工退出" if ( $anykey =~ /q/i );
}

sub process_sql
{
	my ($sql) = @_;

	anykey if ( $DEBUG_MODE =~ /^debug$/i );

	#处理自定义语句
	if (user_define_process($sql)==0){
		$GLOBAL_VALID_SQL = 1;
		return;
	}

	#存在没有被替换的变量时报错
	if ( $sql =~ /\$(\w+)/ ) {
		log_info "D",__FILE__,__LINE__,"SQL: ".$sql;
		log_info "E",__FILE__,__LINE__,"存在非法变量[\$$1]"
	}

	#非自定义语句作为标准SQL语句执行
	sql_exec($sql);
}

sub main
{
	#输入参数校验
	if (@ARGV < 2){
		die "Usage: PerlSQL sqlfile etldate [debug]!";
	}else{
		($SQL_FILE,$ETL_DATE,$DEBUG_MODE) = @ARGV;
	}

	#校验日期
	unless ( date_fun($ETL_DATE,'D',0) ){
		die "日期[$ETL_DATE]非法!";
	}

	#打开sql脚本
	unless(open($FD_SQLFILE,$SQL_FILE)){
		die "Can't open file $SQL_FILE!";
	}

	log_info "I",__FILE__,__LINE__," ======= 脚本:[$SQL_FILE] ETL日期:[$ETL_DATE] =======";

	db_conn;

	init_macro_local;

	my $sql="";
	my $filter_flag=0;
	while ( <$FD_SQLFILE> ){
		my $line = $_;
		$LINE_NO = $LINE_NO + 1;

		#SQL脚本预处理,过滤单行注释--和/* ... */,翻译自定义变量
		$line = sql_line_process($line);

		#过滤跨行注释 /* ... */
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

	log_info "E",__FILE__,__LINE__,"没有一条合法语句,出错退出" if ( $GLOBAL_VALID_SQL == 0 );

	close($FD_SQLFILE); 

	db_disc;
}

main
