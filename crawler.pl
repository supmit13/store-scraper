#!/usr/bin/perl
## ** IMPORTANT ** Change the above path to where ever your perl interpreter is located

use strict;
#use warnings; # Uncomment this for debugging.

use DBI;

sub BEGIN{
	push(@INC, './modules');
}

use Costco;
use Walmart;
use Safeway;

my $zipcodeFile = "";
$zipcodeFile = $ARGV[1] if($ARGV[0] == "-z" && $ARGV[1] =~ /^\w+\.\w{3}$/);
open(ZP, "$zipcodeFile");
my @zipcodes = split(/,/, <ZP>); # These zipcodes will be used only during processing of "safeway.com"
close ZP;

print "Starting to scrape costco.com\n";
my $cr = Costco->new("http://www.costco.com");

my $DBNAME = "ShoperTUR";
my $DBHOST = "50.63.244.197";
my $DBUSER = "ShoperTUR";
my $DBPASSWD = 'FJD#@tBO5';
my $dbh = dbConnect($DBNAME, $DBHOST, $DBUSER, $DBPASSWD);

my $content = $cr->getHomePage();
my $giftsUrl = $cr->getGroceriesLinks($content);
$content = $cr->getPage($giftsUrl);
my $subcatlinksref = $cr->getSubCategoryLinks($content);
my $pageCtr = 1;
my $metaTags = "";
open(FD, ">D:\\work\\odesk\\ScrapeToMysql\\dumpHTML.html");
foreach my $link (@{$subcatlinksref}){
	$content = $cr->getPage($link);
	my $prodPageLinksRef = $cr->getProductsListPageLinks($content);
	my $prodPageContent = $content;
	if (scalar(@{$prodPageLinksRef}) > 0){
		foreach my $prodlink (@{$prodPageLinksRef}){
			#print $prodlink."\n";
			$prodPageContent = $cr->getProductsListPage($prodlink);
			my $dataStore = $cr->extractProductInfo($prodPageContent);
			$metaTags = $cr->getProductMetaTags($prodPageContent);
			print FD $metaTags."\n";
			foreach my $cat (keys %{$dataStore}){
				my $level2 = $dataStore->{$cat};
				foreach my $prodhash (@{$level2}) {
					foreach my $prodname (keys %{$prodhash}) {
						my $registered = chr(174);
						my $trademark = chr(8482);
						my $copyright = chr(169);
						my ($brandname, $prodname2) = ("", "");
						#my $location = $zipcodes[0]; # Hardcoded for now. To be implemented later
						my $location = "";
						($brandname, $prodname2) = split(/$registered/, $prodname) if($prodname =~ /$registered/);
						($brandname, $prodname2) = split(/$trademark/, $prodname) if($prodname =~ /$trademark/);
						($brandname, $prodname2) = split(/$copyright/, $prodname) if($prodname =~ /$copyright/);
						$prodhash->{$prodname}[0] =~ s/\$//g;
						if (!$prodhash->{$prodname}[0]) {
							$prodhash->{$prodname}[0] = "Unavailable";
						}
						my $exists = searchSQL($dbh, $prodname, $cat, 'costco.com');
						if(!$exists){
							my $insert_sql = "insert into products (productName, category, price, srcWebsite, brand, location, unit) values (\"".$prodname."\", \"".$cat."\", \"".$prodhash->{$prodname}->[0]."\", \"costco.com\", \"".$brandname."\", \"".$location."\", \"".$prodhash->{$prodname}->[1]."\")";
							my $insert_sth = $dbh->prepare($insert_sql);
							$insert_sth->execute();
						}
						else{
							my $update_sql = "update products set price=\"".$prodhash->{$prodname}->[0]."\", brand=\"".$brandname."\", location=\"\", unit=\"".$prodhash->{$prodname}->[1]."\" where productName=\"".$prodname."\" and category=\"".$cat."\" and srcWebsite='costco.com'";
							my $update_sth = $dbh->prepare($update_sql);
							$update_sth->execute();
						}
					}
				}
			}
			#close FH;
		}
	}
	else{
		#my $filename = "prodpages\\prodpage_".$pageCtr.".html";
		my $dataStore = $cr->extractProductInfo($prodPageContent);
		$metaTags = $cr->getProductMetaTags($prodPageContent);
		print FD $metaTags."\n";
		foreach my $cat (keys %{$dataStore}){
			my $level2 = $dataStore->{$cat};
			foreach my $prodhash (@{$level2}) {
				foreach my $prodname (keys %{$prodhash}) {
					my $registered = chr(174);
					my $trademark = chr(8482);
					my $copyright = chr(169);
					#my $location = $zipcodes[0]; # Hardcoded for now. To be implemented later
					my $location = "";
					my ($brandname, $prodname2) = split(/$registered/, $prodname) if($prodname =~ /$registered/);
					($brandname, $prodname2) = split(/$trademark/, $prodname) if($prodname =~ /$trademark/);
					($brandname, $prodname2) = split(/$copyright/, $prodname) if($prodname =~ /$copyright/);
					$prodhash->{$prodname}->[0] =~ s/\$//g;
					if (!$prodhash->{$prodname}->[0]) {
						$prodhash->{$prodname}->[0] = "Unavailable";
					}
					$prodhash->{$prodname}->[1] = '1' if(!$prodhash->{$prodname}->[1]);
					my $exists = searchSQL($dbh, $prodname, $cat, 'costco.com');
					if(!$exists){
						my $insert_sql = "insert into products (productName, category, price, srcWebsite, brand, location, unit) values (\"".$prodname."\", \"".$cat."\", \"".$prodhash->{$prodname}->[0]."\", \"costco.com\", \"".$brandname."\", \"".$location."\", \"".$prodhash->{$prodname}->[1]."\")";
						my $insert_sth = $dbh->prepare($insert_sql);
						$insert_sth->execute();
					}
					else{
						my $update_sql = "update products set price=\"".$prodhash->{$prodname}->[0]."\", brand=\"".$brandname."\", location=\"\", unit=\"".$prodhash->{$prodname}->[1]."\" where productName=\"".$prodname."\" and category=\"".$cat."\" and srcWebsite='costco.com'";
						my $update_sth = $dbh->prepare($update_sql);
						$update_sth->execute();
					}
				}
			}
		}
		$pageCtr++;
	}
}
close FD;
print "Done scraping costco.com\n";

print "Starting to scrape walmart.com\n";
my $wm = Walmart->new("http://www.walmart.com");
$content = $wm->getHomePage();
my $groceryUrl = $wm->getGroceryCategoryLink($content);
$content = $wm->getGroceryCategoryPage($groceryUrl);

my $subCategoryLinks = $wm->getSubCatLinks($content);
foreach my $subcatName (keys %{$subCategoryLinks}){
	my $subcatLink = $subCategoryLinks->{$subcatName};
	while(1){
		my $productsListPageContent = $wm->getPage($subcatLink);
		my $prodInfo = $wm->extractProductsInfo($productsListPageContent);
		my $metaKeywords = $wm->getProductMetaTags($productsListPageContent);
		foreach my $prod (@{$prodInfo}){
			my $prodName = $prod->{'title'};
			$metaKeywords .= ", ".$prodName;
			my $cat = $subcatName;
			my $website = "walmart.com";
			my $price = $prod->{'price'};
			my $unit = $prod->{'unit'};
			my $brandname = "";
			#my $location = $zipcodes[0]; # Hardcoded for now. To be implemented later
			my $location = "";
			if(searchSQL($dbh, $prodName, $cat, $website)){ # Do update
				my $update_sql = "update products set price=\"".$price."\", brand=\"".$brandname."\", unit=\"".$unit."\", producttags=\"".$metaKeywords."\" where productName=\"".$prodName."\" and category=\"".$cat."\" and srcWebsite=\"".$website."\" and location=\"".$location."\"";
				my $update_sth = $dbh->prepare($update_sql);
				$update_sth->execute();
			}
			else{ # Do insert
				my $insert_sql = "insert into products (productName, category, price, srcWebsite, brand, location, unit, producttags) values (\"".$prodName."\", \"".$cat."\", \"".$price."\", \"".$website."\", \"".$brandname."\", \"".$location."\", \"".$unit."\", \"".$metaKeywords."\")";
				my $insert_sth = $dbh->prepare($insert_sql);
				$insert_sth->execute();
			}
		}
		my $parser = HTML::TokeParser->new(\$productsListPageContent);
		my $nextFound = 0;
		while(my $nextTag = $parser->get_tag("a")){
			if($nextTag && $nextTag->[1]{'class'} eq "jump next"){
				$nextFound = 1;
				$subcatLink = $wm->{'baseUrl'}.$nextTag->[1]{'href'};
			}
		}
		last if(!$nextFound);
	}
}
print "Done scraping walmart.com\n";

print "Starting to scrape safeway.com\n";
my $sw = Safeway->new("http://shop.safeway.com");
$content = $sw->getHomePage();
$sw->{'cookies'} = $sw->_getCookiesFromResponseHeaders();
#print $sw->{'cookies'};
$sw->{'httpHeaders'}->header('Referer' => 'http://shop.safeway.com/Dnet/SecurityGateway.aspx?type=1&RedirectTo=IconDepartments.aspx');
$content = $sw->getPage("https://shop.safeway.com/ecom/account/sign-in");
foreach my $zipcode (@zipcodes){
	# Enter a zipcode and submit the form.
	$content = $sw->fillupAndSubmitZipcodeForm($content, $zipcode);
	$content = $sw->clickOnShopByAisle();
	# Now at this point we have the category display page.
	# From now, we will iterate over each category and navigate to its 
	# products, and from these products we will derive the desired data.
	my $categoryLinksRef = $sw->getMainCategoryLinks($content);
	my $parser = HTML::TokeParser->new(\$content);
	my %formData = ();
	while(my $inputTag = $parser->get_tag("input")){
		if($inputTag->[1]{'type'} eq 'hidden' && $inputTag->[1]{'name'} eq '__VIEWSTATE'){
			$formData{'__VIEWSTATE'} = $inputTag->[1]{'value'}
		}
		elsif($inputTag->[1]{'type'} eq 'hidden' && $inputTag->[1]{'name'} eq '__EVENTVALIDATION'){
			$formData{'__EVENTVALIDATION'} = $inputTag->[1]{'value'}
		}
		elsif($inputTag->[1]{'type'} eq 'hidden' && $inputTag->[1]{'name'} eq '__EVENTTARGET'){
			$formData{'__EVENTTARGET'} = $inputTag->[1]{'value'}
		}
		elsif($inputTag->[1]{'type'} eq 'hidden' && $inputTag->[1]{'name'} eq '__EVENTARGUMENT'){
			$formData{'__EVENTARGUMENT'} = $inputTag->[1]{'value'}
		}
	}
	my %productData = ();
	foreach my $catLink (keys %{$categoryLinksRef}){
		$formData{'__EVENTTARGET'} = $categoryLinksRef->{$catLink}->[0];
		$formData{'__EVENTARGUMENT'} = '';
		my $id = $categoryLinksRef->{$catLink}->[1];
		$sw->{'pageRequest'} = HTTP::Request->new('GET', 'http://shop.safeway.com/Dnet/Aisles.aspx?ID='.$id, $sw->{'httpHeaders'});
		$sw->{'pageResponse'} = $sw->{'userAgent'}->request($sw->{'pageRequest'});
		$sw->{'currentPageContent'} = $sw->{'pageResponse'}->decoded_content;
		# At this point, currentPageContent contains subcategory items.
		my $subcatDict = $sw->extractSubcategoryInfo($sw->{'currentPageContent'});
		# At this point, currentPageContent contains subcategory items. Parse them and find product pages from these subcategories.
		foreach my $subcatText (keys %{$subcatDict}) {
			my $subId = "";
			if($subcatDict->{$subcatText} =~ /return\s+UpdateFrames\(\'(\d+_\d+)\'\)/){
				$subId = $1;
			}
			my $url = "http://shop.safeway.com/Dnet/Shelves.aspx?ID=".$subId;
			my $requestHeader1 = $sw->{'httpHeaders'};
			$requestHeader1->header('Referer' => 'http://shop.safeway.com/Dnet/Aisles.aspx?ID='.$id);
			$sw->{'pageRequest'} = HTTP::Request->new('GET', $url, $requestHeader1);
			$sw->{'pageResponse'} = $sw->{'userAgent'}->request($sw->{'pageRequest'});
			$sw->{'cookies'} .= $sw->_getCookiesFromResponseHeaders();
			$sw->{'httpHeaders'}->header('Cookie' => $sw->{'cookies'});
			$sw->{'currentPageContent'} = $sw->{'pageResponse'}->decoded_content;
			my $secondLevelSubCatData = $sw->extractSecondLevelSubCategory($sw->{'currentPageContent'});
			foreach my $secondLevelSubCat (keys %{$secondLevelSubCatData}){
				my $requestHeader2 = $requestHeader1;
				$requestHeader2->header('Referer' => $url);
				$sw->{'pageRequest'} = HTTP::Request->new('GET', $secondLevelSubCatData->{$secondLevelSubCat}, $requestHeader2);
				$sw->{'pageResponse'} = $sw->{'userAgent'}->request($sw->{'pageRequest'});
				$sw->{'currentPageContent'} = $sw->{'pageResponse'}->decoded_content;
				my $prodinfo = $sw->getProductsData($sw->{'currentPageContent'});
				my $metaKeywords = $sw->getProductMetaTags($sw->{'currentPageContent'});
				my $website = "safeway.com";
				foreach my $prodName (keys %{$prodinfo}){
					my $price = $prodinfo->{$prodName}->[0];
					my $unit = $prodinfo->{$prodName}->[1];
					my $brandname = "";
					my $cat = $secondLevelSubCat;
					if(searchSQL($dbh, $prodName, $cat, $website)){ # Do update
						my $update_sql = "update products set price=\"".$price."\", brand=\"".$brandname."\", unit=\"".$unit."\", producttags=\"".$metaKeywords."\" where productName=\"".$prodName."\" and category=\"".$cat."\" and srcWebsite=\"".$website."\" and location=\"".$zipcode."\"";
						my $update_sth = $dbh->prepare($update_sql);
						$update_sth->execute();
					}
					else{ # Do insert
						my $insert_sql = "insert into products (productName, category, price, srcWebsite, brand, location, unit, producttags) values (\"".$prodName."\", \"".$cat."\", \"".$price."\", \"".$website."\", \"".$brandname."\", \"".$zipcode."\", \"".$unit."\", \"".$metaKeywords."\")";
						my $insert_sth = $dbh->prepare($insert_sql);
						$insert_sth->execute();
					}
				}
			}
		}
	}
}
print "Done scraping safeway.com\n";
dbDisconnect($dbh);

## ======================== Database connection and operational subroutines =======================

sub dbDisconnect{
	my $dbh = shift;
	$dbh->disconnect();
	undef $dbh;
}

sub dbConnect{
	my $dbName = shift;
	my $dbHost = shift;
	my $dbUser = shift;
	my $dbPasswd = shift;
	my $dsn = "DBI:mysql:".$dbName.":".$dbHost;
	my $dbh = DBI->connect($dsn, $dbUser, $dbPasswd) || die "Could not connect to database: $!\n";
	return $dbh;
}


sub searchSQL{
	my $dbh = shift;
	my $prodName = shift;
	my $category = shift;
	my $website = shift;
	my $select_sql = "select count(*) as count_rec from products where productName=\"".$prodName."\" and srcWebsite=\"".$website."\" and category=\"".$category."\"";
	my $select_sth = $dbh->prepare($select_sql);
	$select_sth->execute();
	my $rec = $select_sth->fetchrow_hashref();
	if($rec->{'count_rec'} > 0){
		return(1);
	}
	else{
		return(0);
	}
}


sub startCrawl{
	my $storeName = shift;
	my $crawlSql = "select store.storeid, crawler.crawlerid, crawler.crawlername, crawler.crawlermodule, store.storeurl from store, crawler where store.storename='".$storeName."' and store.storeid=crawler.storeid";
}