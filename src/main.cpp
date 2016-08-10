#include <iostream>
#include <fstream>
#include <zlib.h>
#include <map>
#include <vector>

#include <chrono>
#include <ctime>
#include "../headers/fileformat.pb.h"
#include "../headers/osmformat.pb.h"
using namespace std;

typedef struct
{
	int id;
	double lat;
	double lon;
	vector<int> adj;
} node_t;

bool processDataBlock( map< int, node_t > &mapData, OSMPBF::Blob* );
bool processHeaderBlock( OSMPBF::Blob* );
void inflateData( OSMPBF::Blob* b, unsigned char* test );
void processDenseNodes( map< int, node_t > &mapData, OSMPBF::PrimitiveBlock* pb, int i );
void processWays( map< int, node_t > &mapData, OSMPBF::PrimitiveBlock* pb, int i );

int adjTotal = 0;

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
	
	map< int, node_t > mapData;
	
	chrono::duration<double> time;
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
        cout << "-----------------------------------" << endl;

        blobHeaderBuffer = realloc( blobHeaderBuffer, headerLen );
        input.read( reinterpret_cast<char *>(blobHeaderBuffer), headerLen );
        blobHeader.ParseFromArray( blobHeaderBuffer, headerLen );

        blobBuffer = realloc( blobBuffer, blobHeader.datasize() );
        input.read( reinterpret_cast<char * >(blobBuffer), blobHeader.datasize() );
        blob.ParseFromArray( blobBuffer, blobHeader.datasize() );

        cout << "I: " << i << endl;
        i++;

        if( strcmp("OSMData", blobHeader.type().c_str()) == 0 )
        {
            processDataBlock( mapData, &blob );
        }
        else if( strcmp("OSMHeader", blobHeader.type().c_str()) == 0 )
        {
            processHeaderBlock( &blob );
        }
    } while( 1 );
	
	end = chrono::system_clock::now();
	time = end - start;
	
	cout << "Map Size: " << mapData.size() << endl;
	cout << "Adj Size: " << adjTotal << endl;
	cout << "Time: " << time.count() << endl << endl;
    input.close();
    free(blobHeaderBuffer);
    free(blobBuffer);

    //cout << endl << "Blocks: " << i << endl;
}

bool processDataBlock( map< int, node_t > &mapData, OSMPBF::Blob* b )
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
			//cout << "Has dense" << endl;
			processDenseNodes( mapData, &pb, i );
		}
		else if( pb.primitivegroup( i ).nodes_size() > 0)
		{
			//processNodes();
			cout << "Nodes" << endl;
		}
		else if( pb.primitivegroup( i ).ways_size() > 0 )
		{
			processWays( mapData, &pb, i);
			//cout << "Ways: " << pb.primitivegroup( i ).ways_size() << endl;
		}
	}
	
	free( uncompressed );
	uncompressed = NULL;
    return true;
}

void processWays( map< int, node_t > &mapData, OSMPBF::PrimitiveBlock* pb, int i )
{
	//local variables
	OSMPBF::PrimitiveGroup pg = pb->primitivegroup( i );
	OSMPBF::Way w;
	int j = 0;
	int numWays = 0;
	int numRefs = 0;
	int ref1;
	int ref2;
	
	for(j = 0, numWays = pg.ways_size(); j < numWays; j++) 
	{
		w = pg.ways(j);
		ref1 = 0;
		ref2 = w.refs( 0 );
        for(int k = 0, numRefs = w.refs_size(); k < numRefs - 1; k++)
		{
			ref1 += w.refs( k );
			ref2 += w.refs( k + 1 );
            mapData[ ref1 ].adj.push_back( ref2 );
			mapData[ ref2 ].adj.push_back( ref1 );
			adjTotal += 2;
        }
    }
}

void processDenseNodes( map< int, node_t > &mapData, OSMPBF::PrimitiveBlock* pb, int i )
{
	//local variables
	OSMPBF::DenseNodes dn = pb->primitivegroup( i ).dense();
	long int id = 0;
	double lon = 0;
    double lat = 0;
	double lon_offset = pb->lon_offset();;
	double lat_offset = pb->lat_offset();;
	double gran = pb->granularity();;
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
		mapData[ id ] = thisNode;
	}
}

bool processHeaderBlock( OSMPBF::Blob* b )
{
    return true;
}

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

	cout << "Total Out: " << z.total_out / 1024 << " KB" << endl;
}







