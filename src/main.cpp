#include <iostream>
#include <fstream>
#include <zlib.h>
#include <map>
#include <vector>
#include <math.h>
#include <chrono>
#include <ctime>


#include <bsoncxx/builder/stream/document.hpp>
#include <bsoncxx/builder/stream/array.hpp>
#include <bsoncxx/types.hpp>
#include <bsoncxx/json.hpp>

#include <mongocxx/client.hpp>
#include <mongocxx/options/find.hpp>
#include <mongocxx/instance.hpp>
#include <mongocxx/uri.hpp>

#include "../headers/fileformat.pb.h"
#include "../headers/osmformat.pb.h"
#include "../headers/osmParser.h"
using namespace std;


/*-------------------------------------------
main()
Handles opening the file and passing it to child
functions for parsing and data storage
--------------------------------------------*/
int main( int argc, char* argv[] )
{
    //error checking
    if( argc != 2)
    {
        cout << "Usage: " << argv[0] << " <filename.pbf>" << endl;
        return 0;
    }

    //local variables
    ifstream input( argv[1], ios::in | ios::binary );
    int headerLen = 0;
    void* blobHeaderBuffer = malloc(0);
    void* blobBuffer = malloc(0);
    OSMPBF::BlobHeader blobHeader;
    OSMPBF::Blob blob;
    int i = 0;
	map< nodeId_t, node_t > fullData;
	map< nodeId_t, node_t > reducedData;
	chrono::duration<double> time1, time2;
	chrono::time_point<chrono::system_clock> start, end;
	
	start = chrono::system_clock::now();
	
    do
    {
        input.read( reinterpret_cast<char *>(&headerLen), sizeof(int) );
        if( input.eof() )
        {
			break;
		}    

        headerLen = __builtin_bswap32( headerLen );

        blobHeaderBuffer = realloc( blobHeaderBuffer, headerLen );
        input.read( reinterpret_cast<char *>(blobHeaderBuffer), headerLen );
        blobHeader.ParseFromArray( blobHeaderBuffer, headerLen );

        blobBuffer = realloc( blobBuffer, blobHeader.datasize() );
        input.read( reinterpret_cast<char * >(blobBuffer), blobHeader.datasize() );
        blob.ParseFromArray( blobBuffer, blobHeader.datasize() );

		if( i % 100 == 0)
			cout << "Block: " << i << endl;
        i++;

        if( strcmp("OSMData", blobHeader.type().c_str()) == 0 )
        {
            processDataBlock( fullData, &blob );
        }
        else if( strcmp("OSMHeader", blobHeader.type().c_str()) == 0 )
        {
            processHeaderBlock( &blob );
        }
    } while( 1 );
	
	postProcessNodes( fullData, reducedData );
	end = chrono::system_clock::now();
	time1 = end - start;
	
	start = chrono::system_clock::now();
	bulkWriteToDB( reducedData );
	end = chrono::system_clock::now();
	time2 = end - start;
	
    input.close();
    free(blobHeaderBuffer);
    free(blobBuffer);

	cout << endl <<"Blocks: " << i << endl;
	cout << "Map Size: " << fullData.size() / 1000 << "k" << endl;
	cout << "Reduced Size: " << reducedData.size() / 1000 << "k" << endl << endl;
	
	
	cout << "Parse time: " << time1.count() << " seconds" << endl;
	cout << "Write time: " << time2.count() << " seconds" << endl << endl;
}


/*-------------------------------------------
bulkWriteToDB()
Connect to mongoDB on localhost and insert 
data in a single bulk write
--------------------------------------------*/
bool bulkWriteToDB( map< nodeId_t, node_t > &data )
{
	using bsoncxx::builder::stream::document;
	using bsoncxx::builder::stream::array;
	using bsoncxx::builder::stream::finalize;
	
	cout << "Writing " << data.size() / 1000 << "k to DB..." << endl;
	mongocxx::instance inst{};
	mongocxx::uri uri{};
	mongocxx::client conn{ mongocxx::uri{} };
	
	auto coll = conn["abc"]["osmStuff"];
	coll.drop();
	mongocxx::bulk_write bulk{};
	
	map<nodeId_t, node_t>::iterator node;
	int j = 0;
	for( node = data.begin(); node != data.end(); node++ )
	{	
		node_t* n = &node->second;
		bsoncxx::builder::stream::document nodeDoc = document{};
		nodeDoc << "id" << n->id << "lat" << n->lat << "lon" << n->lon;
		
		auto adjArr = array{};
		for( j = 0; j < n->adj.size(); j++ )
		{
			auto adjDoc = document{};
			adjDoc << "adj" << n->adj[j].first << "dist" << n->adj[j].second << finalize;
			adjArr << bsoncxx::types::b_document{adjDoc.view()};
		}
		
		nodeDoc << "adj" << bsoncxx::types::b_array{adjArr} << finalize;
		mongocxx::model::insert_one op{nodeDoc.view()};
		bulk.append(op);
	}
	
	mongocxx::stdx::optional<mongocxx::result::bulk_write> result = coll.bulk_write(bulk);
	
	if( !result )
        return false;
	else if( result->inserted_count() == data.size() )
		return true;
}


/*-------------------------------------------
postProcessNodes()
Reduces the data in fullData. Nodes with 2 or
less adjacencies can be removed from the data
as these are not intersections. Each adjacency
gets a distance associated with it before
throwing away the fullData
--------------------------------------------*/
void postProcessNodes( map<nodeId_t, node_t> &fullData, map<nodeId_t, node_t> &reducedData )
{
	//local variables
	map<nodeId_t, node_t>::iterator i;
	map<nodeId_t, node_t>::iterator j;
	node_t n1;
	node_t n2;

	for( i = fullData.begin(); i != fullData.end(); i++)
	{
		if( i->second.adj.size() > 2 )
		{
			reducedData[i->second.id] = i->second;
		}
	}
	
	for( j = reducedData.begin(); j != reducedData.end(); j++)
	{
		n1 = j->second;

		for(int k = 0; k < n1.adj.size(); k++)
		{
			n2 = fullData[ n1.adj[k].first ];
			n1.adj[k].second = calculateDist(n1.lat, n1.lon, n2.lat, n2.lon);
			
		}
	}

}


/*-------------------------------------------
proccessDataBlock()
Takes a blob and determines what processing
needs to be done on it. If compressed, will
call for decompression and then pass to
the specific processing function for its 
OSM type
--------------------------------------------*/
bool processDataBlock( map< nodeId_t, node_t > &fullData, OSMPBF::Blob* b )
{
	//local variables
	OSMPBF::PrimitiveBlock pb;
	unsigned char* uncompressed = (unsigned char*) malloc( b->raw_size() );
	int pbSize = 0;
	int i = 0;
	
	if( b->has_zlib_data() )
	{
		inflateData( b, uncompressed );
	}
	else if( b->has_raw() )
	{
		memcpy( uncompressed, b->raw().c_str() , b->raw_size() );
	}
	else
	{
		cout << "Error with compression type" << endl;
		return false;
	}
	
	pb.ParseFromArray( uncompressed, b->raw_size() );
	
	for( i = 0, pbSize = pb.primitivegroup_size(); i < pbSize; i++ )
	{
		if( pb.primitivegroup( i ).has_dense() )
		{
			processDenseNodes( fullData, &pb, i );
		}
		else if( pb.primitivegroup( i ).nodes_size() > 0)
		{
			//processNodes();
			cout << "Nodes" << endl;
		}
		else if( pb.primitivegroup( i ).ways_size() > 0 )
		{
			processWays( fullData, &pb, i);
		}
	}
	
	free( uncompressed );
	uncompressed = NULL;
    return true;
}

/*-------------------------------------------
processWays()
Takes a primitive block of ways and adds the
necessary adjacencies to the nodes. Nodes
next to each other in a way share an adjacency
--------------------------------------------*/
void processWays( map< nodeId_t, node_t > &fullData, OSMPBF::PrimitiveBlock* pb, int i )
{
	//local variables
	OSMPBF::PrimitiveGroup pg = pb->primitivegroup( i );
	OSMPBF::Way w;
	int j = 0;
	int numWays = 0;
	int numRefs = 0;
	nodeId_t ref1;
	nodeId_t ref2;
	pair<nodeId_t, dist_t> p1;
	pair<nodeId_t, dist_t> p2;
	
	for(j = 0, numWays = pg.ways_size(); j < numWays; j++) 
	{
		w = pg.ways(j);
		ref1 = 0;
		ref2 = w.refs( 0 );
        for(int k = 0, numRefs = w.refs_size(); k < numRefs - 1; k++)
		{
			ref1 += w.refs( k );
			ref2 += w.refs( k + 1 );
			
			if(ref1 != ref2)
			{
				p1 = make_pair( ref1, 0 );
				p2 = make_pair( ref2, 0 );
				fullData[ ref1 ].adj.push_back( p2 );
				fullData[ ref2 ].adj.push_back( p1 );
			}
        }
    }
}


/*-------------------------------------------
processDenseNodes()
Takes a primitive block of dense nodes and
parses the data to be stored in fullData
--------------------------------------------*/
void processDenseNodes( map< nodeId_t, node_t > &fullData, OSMPBF::PrimitiveBlock* pb, int i )
{
	//local variables
	OSMPBF::DenseNodes dn = pb->primitivegroup( i ).dense();
	nodeId_t id = 0;
	coord_t lon = 0;
    coord_t lat = 0;
	coord_t lon_offset = pb->lon_offset();;
	coord_t lat_offset = pb->lat_offset();;
	coord_t gran = pb->granularity();;
	int nodeSize = 0;
	int j = 0;
	
	for( j = 0, nodeSize = dn.id_size(); j < nodeSize; j++)
	{
		id += dn.id( j );
		lon +=  0.000000001 * ( lon_offset + (gran * dn.lon(j)) );
        lat +=  0.000000001 * ( lat_offset + (gran * dn.lat(j)) );
		
		node_t thisNode;
		thisNode.id = id;
		thisNode.lon = lon;
		thisNode.lat = lat;
		fullData[ id ] = thisNode;
	}
}


/*-------------------------------------------
processHeaderBlock()
Takes a blob of type OSMHeader. Currently
unused
--------------------------------------------*/
bool processHeaderBlock( OSMPBF::Blob* b )
{
    return true;
}


/*-----------------------------------------------
inflateData()
Takes a blob and decompresses blob->zlib_data
and puts it in uncompressed. Blob->raw_size bytes
are decompressed 
-----------------------------------------------*/
void inflateData( OSMPBF::Blob* b, unsigned char* uncompressed )
{
	//local variables
	z_stream z;
	int uncompresedSize = b->raw_size();
	int compressedSize = b->zlib_data().size();
	
	//initialize variables
	z.zalloc = Z_NULL;
	z.zfree = Z_NULL;
	z.opaque = Z_NULL;
	z.avail_in = compressedSize;
	z.next_in = (unsigned char*) b->zlib_data().c_str();
	z.avail_out = uncompresedSize;
	z.next_out = uncompressed;
	
	if(inflateInit(&z) != Z_OK) {
        cout << "failed to init zlib stream" << endl;
    }
    if(inflate(&z, Z_FINISH) != Z_STREAM_END) {
        cout << "failed to inflate zlib stream" << endl;
    }
    if(inflateEnd(&z) != Z_OK) {
        cout << "failed to deinit zlib stream" << endl;
    }
}


/*-------------------------------------------
calculateDist()
Calculates the distance on earth between
two sets of coordinates. Distance is in km
--------------------------------------------*/
dist_t calculateDist(coord_t lat1, coord_t lon1, coord_t lat2, coord_t lon2)
{
    double lat1Rad = toRad( lat1 );
	double lon1Rad = toRad( lon1 );
    double lat2Rad = toRad( lat2 );
	double lon2Rad = toRad( lon2 );
	
    double u = sin( ( lat2Rad - lat1Rad ) / 2 );
    double v = sin( ( lon2Rad - lon1Rad ) / 2 );

    double d = 2.0 * R * asin(sqrt(u * u + cos(lat1Rad) * cos(lat2Rad) * v * v));
	
    return d;
}


/*-------------------------------------------
toRad()
Convert degrees to radians
--------------------------------------------*/
double toRad( coord_t deg)
{
	return ( deg * PI / 180 );
}







