/* 
 * File:   main.cpp
 * Author: sethuraman
 *
 * Created on October 18, 2013, 10:32 PM
 */

#include <cstdlib>
#include <iostream>
#include <list>
#include <new> 
#include <random>
#include <cmath>
#include <ctime> 
#include <chrono>
#include "randomc.h"
#include "mother.cpp"
#include <fstream>

/*  Timer class used to find the execution time as clock() did not give appropriate results    */

using namespace std;

class Timer
{
public:
    Timer() : beg_(clock_::now()) {}
    void reset() { beg_ = clock_::now(); }
    double elapsed() const { 
        return std::chrono::duration_cast<second_>
            (clock_::now() - beg_).count(); }

private:
    typedef std::chrono::high_resolution_clock clock_;
    typedef std::chrono::duration<double, std::ratio<1> > second_;
    std::chrono::time_point<clock_> beg_;
};

/*  Edge structure of the graph saves destination, weight and pointer to the next edge of that vertex    */

typedef struct Edge{
int y; 
int weight; 
struct Edge  *next; 
} edgenode;

/*  FIbonacci Heap node structure     */

struct fibHpNode
{
        int key,deg;
        fibHpNode *left;
        fibHpNode *right;
        fibHpNode *parent;
        fibHpNode *child;
        bool childcut;
        int index;        
};

/*  FIbonacci Heap Class     */

class fibheap
{
	private :
            int rcount,rnos;
            fibHpNode *heapmin;                       
            fibHpNode **ref;
            int mx;        
                        
    public  :

            fibheap(int siz)                        //Fibheap constructor
            {
                heapmin = NULL;
                rcount=0;rnos=0;mx=0;
                ref=new fibHpNode*[siz];
            }

/*  //Inserting into a Fibonacci heap
      //called only by the fibonacci heap during cascade cut procedure*/
                                
            void hinsert(fibHpNode *hnode)                
            {                        
                if(heapmin==NULL)
                {
                    heapmin=hnode;
                }        
                else
                {
                    hnode->left=heapmin->left;
                    hnode->right=heapmin;
                    hnode->left->right=hnode;                                
                    heapmin->left=hnode;
                    if(heapmin->key>hnode->key)
                    {
                        heapmin=hnode;
                    }
                }
                ref[hnode->index]=hnode;                                //Maintaining a reference array
            }

/*  //Inserting into a Fibonacci heap
      //called only during heap initialization*/
	     	void insert(fibHpNode *hnode)               
            {                        
                if(heapmin==NULL)
                {
                   heapmin=hnode;
                }        
                else
                {
                   hnode->left=heapmin->left;
                   hnode->right=heapmin;
                   hnode->left->right=hnode;                                
                   heapmin->left=hnode;
                   if(heapmin->key>hnode->key)
                   {
                        heapmin=hnode;
                   }
                }
                rcount=rcount+1;                        //Maintaining the count of number of nodes
                mx=mx+1;
                ref[hnode->index]=hnode;                                //Maintaining a reference array
            }

//Deleting the Minimum Element from Fibonacci Heap
            int* hdeletemin()                                
            {
                fibHpNode *del;
                fibHpNode *delch,*temp;
                int *keyInd=new int[2];
                del=heapmin;
                //ref[heapmin->index]=NULL;
                if(del!=NULL)
                {
			        keyInd[0]=heapmin->key;
                    keyInd[1]=heapmin->index;
                    delch=heapmin->child;
                    for(int i=1;i<=(del->deg);i++)
                    {
						temp = delch->right;
						delch->left->right = delch->right;
					    delch->right->left = delch->left;
						// add delch to root list of heap
						delch->left =del;  
						delch->right = del->right;
			            del->right = delch;
                		delch->right->left = delch;
						// set parent[x] to null
                		delch->parent = NULL;
                		delch = temp;
                    }
                    heapmin->left->right=heapmin->right;
					heapmin->right->left=heapmin->left;				
                    if(del==del->right)
                    {
                        heapmin=NULL;
                    }
                    else
                    {
						heapmin=del->right;
                        consolidate();
                    }
                    rcount=rcount-1;
                    return keyInd;
                }
                return NULL;
            }

//Consolidate function to perform Pairwise combination 
        void consolidate()                                        
        {
            fibHpNode *a,*b,*c;
            //Calculating the maxdegree based on the number of nodes
            double oneOverLogPhi =1.0 / log((1.0 + sqrt(5.0)) / 2.0);
            int maxdegree=((int) floor(log(rcount) * oneOverLogPhi)) + 1;
			fibHpNode *darray[maxdegree];
            for(int i=0;i<maxdegree;i++)
                darray[i]=NULL;
            a=heapmin;
            rnos=1;
            if(heapmin==NULL)        
                rnos=0;
            else
            {        
                b=a->right;
                while(b!=a)        
                {
                    rnos++;
                    b=b->right;
                }
            }
            
			for(int i=0;i<rnos;i++)
            {
                int d=a->deg;
                c=a->right;
                fibHpNode* y;
                while(darray[d]!=NULL)
                {        
                    y=darray[d];
                    if(a->key>y->key)
                    {
                        fibHpNode *temp=a;
                        a=y;
                        y=temp;
                    }
                    y->left->right=y->right;                 //Removing from rootlist
                    y->right->left=y->left;
                    y->right=y;
                    y->left=y;
                    if(a->child==NULL)                        //Making y child of a
                    {
                        a->child=y;
                        a->child->left=y;
                        a->child->right=y;        
                        y->parent=a;
                        y->left=y;
                        y->right=y;
                    }
                    else
                    {
                        y->left=a->child->left;
                        y->right=a->child;
                        a->child->left->right=y;
                        a->child->left=y; 
                        y->parent=a;
                    }
                    a->deg++;
                    y->childcut=0;
                    darray[d]=NULL;
                    d++;
                }
                darray[d]=a;
	     		a=c;
            }
            heapmin=NULL;
            for(int i2=0;i2<maxdegree;i2++)
            {
                if(darray[i2]!=NULL)
                {
                   if(heapmin==NULL)
                   {
                        darray[i2]->parent=NULL;
                        darray[i2]->left=darray[i2];
                        darray[i2]->right=darray[i2];
                        heapmin=darray[i2];
                    }
                    else
                    {
                        darray[i2]->left=heapmin->left;
                        darray[i2]->right=heapmin;
                        darray[i2]->left->right=darray[i2];
                        heapmin->left=darray[i2];
                        if(darray[i2]->key<heapmin->key)
                        {
                            heapmin=darray[i2];
                        }
                    }
                }
            }       
        }

//Decrease Key function for Fibonacci Heap
        int decrease_key(int w, int e)                        
        {
            fibHpNode *dk;
            fibHpNode *dkp;
            if(e>=ref[w]->key)
            {
                return 0;
            }
            if(e==-1)
            {
                ref[w]->key=ref[w]->key-ref[w]->key;
                heapmin=ref[w];
            }
            else
                ref[w]->key=e;
                dk=ref[w];
                dkp=ref[w]->parent;
                if((dk->parent!=NULL)&&(dk->key<dk->parent->key))
                {
                    cut(dk);                                //Removing the node
                    cascade_cut(dkp);                        //Cascade cut operation
                }
                return 1;
        }

//Cut operation to remove the node from the fibonacci heap

        void cut(fibHpNode *om)                                        
        {
            fibHpNode *p;
            p=om->parent;
            if(p->child==om)
            { 
                if(om->right==om)
                {
                    p->child=NULL;
                }
                else
                {
                    p->child=om->right;
                    om->left->right=om->right;
                    om->right->left=om->left;
                    om->left=om;
                    om->right=om;
                }
			}
            else
            {
                om->left->right=om->right;
                om->right->left=om->left;
                om->left=om->right=om;
            }
            p->deg--;
            om->parent=NULL;
            om->childcut=0;
            hinsert(om);
        }

//Cascade function to check childcut values
        void cascade_cut(fibHpNode *pm)                                
        {
            fibHpNode *ppm;
            ppm=pm->parent;
            if(pm->parent!=NULL)                         
            {
                if(pm->childcut==0)
                {
                    pm->childcut=1;
                }
				else
				{
					cut(pm);
					cascade_cut(ppm);
				}
			}
        }

        void remove(int x)                                        //Remove function based on index value
        {
            decrease_key(x,-1);
            hdeletemin();
        }

};

class graph
{

	/*
	Graph maintains list of edges for each vertex , the degree of the graph, the number of vertices
	*/

	private:

	struct Edge **edges; 
	int *degree; int numVertices; 
	unsigned long long int numEdges; 
	fibheap *fib;
    
/*        Insert  edges source, destination, weight
            Inserts twice  for both source and destination vertices                            */
	

	void insert_edge(int x, int y, int wei, int run)
	{
		struct Edge *p; /* temporary pointer */
		p=new struct Edge;
		p->weight = wei;
		p->y = y;
		p->next = this->edges[x];
		this->edges[x] = p; /* insert at head of list */
		this->degree[x] ++;
		if (run ==0)
			insert_edge(y,x,wei,1);
		else
			this->numEdges ++;
	}

	public :
	
	graph(int d, int e)
	{
		int i; 
		edges= new edgenode*[d];
		degree=new int[d];
		this->numVertices = d;
		this->numEdges = 0;
		for (i=0; i<d; i++) this->degree[i] = 0;
		for (i=0; i<d; i++) this->edges[i] = NULL;
		this->fib=new fibheap(this->numVertices);
		fibHpNode* hnode;
		for (i=0;i<this->numVertices;++i)
		{
    			hnode=new fibHpNode;
    			hnode->key=2000;
    			hnode->deg=0;
    			hnode->parent=NULL;                        
    			hnode->child=NULL;
    			hnode->childcut=0;
    			hnode->left=hnode;
    			hnode->right=hnode;
    			hnode->index=i;
    			this->fib->insert(hnode);
    			//delete hnode;
		}

	}

	graph()
	{
	}

	void construct(char *v)
	{
		cout<<"constructing....................";
		ifstream inf;
        inf.open(v);
		int x, y,wei,i,d,e;
		// If we couldn't open the output file stream for writing
        if (!inf)
    	{
        		// Print an error and exit
        	cerr << filename<<"could not be opened for writing!" << endl;
        	exit(0);
    	}
    	inf>>d>>e;
		edges= new edgenode*[d];
		degree=new int[d];
		this->numVertices=d;
		this->numEdges = 0;
		for (i=0; i<d; i++) this->degree[i] = 0;
		for (i=0; i<d; i++) this->edges[i] = NULL;
		this->fib=new fibheap(this->numVertices);
		fibHpNode* hnode;
		for (i=0;i<d;++i)
		{
    			hnode=new fibHpNode;
    			hnode->key=2000;
    			hnode->deg=0;
    			hnode->parent=NULL;                        
    			hnode->child=NULL;
    			hnode->childcut=0;
    			hnode->left=hnode;
    			hnode->right=hnode;
    			hnode->index=i;
    			this->fib->insert(hnode);
    			//delete hnode;
		}
		for(int i=0;i<e;i++)
		{
			inf>>x>>y>>wei;
			insert_edge(x,y,wei,0);			
		}  
    		
	}

/*

Random Construction : If the density is not 100 random edges are inserted into the graph.
else: a complete graph of degree this->numVertices is constructed with random edge weights 

*/
	int randomConstruct(int percent) 
	{
    		if (percent<(200/this->numVertices+1))
        		return -1;
    		else
    		{
        		int edg1,edg2;
        		time_t st=time(0);
                int **matrix;
      	 		matrix= new int*[this->numVertices];
        		for (int i=0; i<this->numVertices;i++)
            			matrix[i]=new int[this->numVertices];
        		unsigned long long int numIns=1;
       			CRandomMother ran(987651), ran2(56575), *ran3;
        		int seed=7657*(unsigned)time(0);
        		ran3=new CRandomMother(7657*(unsigned)time(0));
				this->numEdges=(unsigned long long int)((this->numVertices-1)*(this->numVertices)/200*percent);
				if (percent==100)
      			{
					unsigned long long int total= (int)((this->numVertices-1)*(this->numVertices)*(100-percent)/200);
                    /*   construction of a complete graph  */				
					for(int m=0;m<this->numVertices;m++) 
            			for(int n=0; n<m; n++)
							matrix[m][n]=matrix[n][m]=ran3->IRandom(1,1000);    
					while(total>0)
        			{
            			edg1=ran.IRandom(0,this->numVertices*this->numVertices-1);
            			edg2=edg1%this->numVertices;
            			edg1=edg1/ this->numVertices;
						while(matrix[edg1][edg2]!=0)
						{
							// generate new destination as self loop and duplicates edges are not permitted
                			edg1=ran.IRandom(0,this->numVertices*this->numVertices-1);
                			edg2=edg1%this->numVertices;
                			edg1=edg1/ this->numVertices;
						} 
            			matrix[edg1][edg2]=matrix[edg2][edg1]=0;
            			--total;
         			}
				}
				else
				{
                	//cout<<"the number is  "<<this->numEdges<<endl;
        			while(numIns<=(this->numEdges))
        			{
                		edg1=(numIns++)%(this->numVertices);
            			edg2=ran.IRandom(0,this->numVertices-1);
           	    		while((edg1==edg2) || (matrix[edg1][edg2]!=0))
            			{
						// generate new destination as self loop and duplicates edges are not permitted
                		edg2=ran2.IRandom(0,this->numVertices-1);
           				}
        	    		if(numIns%this->numVertices==0) ran3=new CRandomMother(seed/7657*(unsigned)time(0));
        		  		matrix[edg1][edg2]=matrix[edg2][edg1]=ran3->IRandom(1,1000);                
        			}
				}
        		for(int m=0;m<this->numVertices;m++) 
            		for(int n=0; n<m; n++)
                		if(matrix[m][n])
                			{
                   				this->insert_edge(m,n,matrix[m][n],0);
                   				//cout<<m<<" "<<n<<"  "<<matrix[m][n]<<endl;
                			}
        		return 1;
    
    		}

	}

/*   
Print graph used to verify the correctness of the graph construction algorithm
void print_graph()
{
int i; 
edgenode *p; 
for (i=0; i<this->numVertices; i++) 
{
cout<<i<<" : ";
p = this->edges[i];
while (p != NULL) 
{
cout<<p->y<<" weight: "<<p->weight<<" , ";
p = p->next;
}
cout<<endl;
}
}
*/

/*

Prims using fibonacci heap
1. Insert all the vertices into the fibonacci heap with infinite key 
2. Decrese key of start vertex to 0.
3. Remove Min [and add that to the MST ] and decrease the key of its neighbours [if not in MST] with its corresponding distance
4. Repeat the step 3 for n-1 times

*/

void fibPrim(int start, int mode)
{
	int i;
	int distance[this->numVertices];
	int parent[this->numVertices];
	bool intree[this->numVertices];
	edgenode *p;
	int *keyInd=new int[2];
	int v;
	int w;
	int weight;
	unsigned long int dist=0;
	for (int i=0; i<this->numVertices; i++) {
		intree[i] = 0;
		parent[i] = -1;
		distance[i]=1002;
		}
	this->fib->decrease_key(start,0);
	v = start;
	distance[start] = 0;
	keyInd=this->fib->hdeletemin();
	for(int j=0;j<this->numVertices-1;j++) {
		intree[v] = 1;
		p = this->edges[v];
		while (p != NULL) {
		w = p->y;
		weight = p->weight;
		if (intree[w] == 0)
		if( this->fib->decrease_key(w,weight)) 
		{    
    			parent[w] = v;
		}
		p = p->next;
		}
		keyInd=this->fib->hdeletemin();
		if(keyInd!=NULL)	
		{
			v = keyInd[1];
			dist += keyInd[0];
			distance[v]=keyInd[0];
		}
	}
if(mode)
		{		
		cout<<dist<<endl;
		for (i=0;i<this->numVertices;i++)
    			cout<<i<<" "<<parent[i]<<"  "<<distance[i]<<endl;
		}
}


/*

1. Prims using simple scheme
2. Initialize a tree with a the start vertex, passed to the function
3. Grow the tree by one edge at a time: Of all the edges that connect the tree to vertices not yet in the tree, find the minimum-weight edge, and transfer it to the tree.
4. Repeat step 2 until all vertices are added to the tree.

*/


	void prim(int start, int mode)	
	{
		int i;
		edgenode *p;
		int parent[this->numVertices];
		bool intree[this->numVertices];
		int distance[this->numVertices];
		int v;
		int w;
		int weight;
		int dist;

		for (int i=0; i<this->numVertices; i++) 
		{
			intree[i] = 0;
			distance[i] = 1002;
			parent[i] = -1;
		}
		distance[start] = 0;
		v = start;
		while (intree[v] == 0) 
		{
			intree[v] = 1;
			p = this->edges[v];
			while (p != NULL) 
			{
			    w = p->y;
			    weight = p->weight;
			    if ((intree[w] == 0)&&(distance[w] > weight) ) {
			     	distance[w] = weight;
				    parent[w] = v;
				}
				p = p->next;
		    }
		    v = 1;
			dist = 1002;
			for (i=0; i<this->numVertices; i++)
			if ((intree[i] == 0) && (dist > distance[i])) {
				dist = distance[i];
				v = i;
			}
		}
		dist=0;
		if(mode)
		{		
			for (i=0;i<this->numVertices;i++)
    			dist+=distance[i];
			cout<<dist<<endl;
			for (i=0;i<this->numVertices;i++)
    			cout<<i<<" "<<parent[i]<<"  "<<distance[i]<<endl;
		}

  	}


/*

Breath first traversal to check whether the graph is connected!! 


*/
	int bfs(int start)
	{
		int numProc=0;    
		bool checked[this->numVertices];
		bool found[this->numVertices];
		int parent[this->numVertices];

		for (int i=0; i<this->numVertices; i++) {
		checked[i] = found[i] = 0;
		parent[i] = -1;
		}
		std::list<int> queue;
		int v;
		int y;
		edgenode *p;
		queue.push_back(start);
		found[start] = 1;
		while (!queue.empty()) {
			v = queue.front();
			queue.pop_front();
			//numProc++;
			checked[v] = 1;
			p = this->edges[v];
			while (p != NULL) {
				y = p->y;
				if (found[y] == 0) {
					queue.push_back(y);
					found[y] = 1;
					parent[y] = v;
					}
				p = p->next;
			}
		}
		for (int i=0;i<this->numVertices;i++)
    			numProc+=checked[i];
		return numProc==this->numVertices;
	}


};

int main(int argc, char** v) {
  
    
    
    switch(v[1][1])
    {
	/*   Random mode   */
		case 'r' :
        {   
            int vert,edges;  
    	    vert=std::stoi(v[2]);
			edges=std::stoi(v[3]);
    	    graph *d;
    	    while (1)
                {
                    d=new graph(vert,0);
                    if( (d->randomConstruct(edges)==-1) )
                    {
                        cout<<"graph cannot be constructed with such a density";
						exit(0);
                        break;
                    }
                    else
					{
                        while(!d->bfs(0))
						{
							cout<<"random graph construction not complete!!"<<endl;
							delete d;
							d=new graph(vert,0);
							d->randomConstruct(edges);
						}
                                            
                    }
                    break;
                                    
				}
			cout<<"random graph construction completed!!"<<endl;
			//d->print_graph();
            Timer tmr; double t;
            d->prim(0,0);
            t=tmr.elapsed();
            cout<<"the prim's took:"<<t<<endl;
            Timer tmr2;
            d->fibPrim(0,0);
            t=tmr2.elapsed();
            cout<<"the fib' prim's took: "<<t<<endl;           
			exit(0);            	
		break;
		        
	}
    /* User Input mode - simple scheme*/
    case  's' :
    {
        graph *d;
        d=new graph;
        cout<<v[2]<<endl;
        d->construct(v[2]);
        if( !d->bfs(0) )
        {
            cout<<"graph cannot be constructed with such a density";
            break;
        }
        else 
        {
		   d->prim(0,1);
		}                

        break;
                                    
    }
    /* User Input mode - fibonacci heap scheme */        
	case 'f':	
	{
        graph *d;
	    d=new graph();
        d->construct(v[2]);
        if(!d->bfs(0) )
        {
            cout<<"graph cannot be constructed with such a density";
            break;
        }
        else 
        {
			d->fibPrim(0,1);
	    }                
        break;
                                    
    }
            
    default:
    {
        cout<<"nothing to say!!";
        break;
                   
    }
 }
   return 0;
 }


