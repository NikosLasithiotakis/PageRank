#define  _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>

#define MAX_NODES 40000

struct node {
    long id;
    double pagerank;
    int neighbour_counter;
    struct node* next;
};

struct thread_arguments {
    int start_node;
    int end_node;
};

struct node* node_list[MAX_NODES];
int max_node_id = 0;
pthread_barrier_t barrier;

struct node* create_node(long id){
    struct node* new_node;
    if(id < 0 || id > MAX_NODES){
        printf("Error: Node ID out of bounds.\n");
        exit(1);
    }
    new_node = (struct node*)malloc(sizeof(struct node));
    if(new_node == NULL){
        printf("Error: Memory allocation failed.\n");
        exit(1);
    }
    new_node->id = id;
    new_node->pagerank = 1.0;
    new_node->neighbour_counter = 0;
    new_node->next = NULL;
    node_list[id] = new_node;
    return new_node;
}

void add_neighbour(struct node* current_node, struct node* neighbour_node){
    struct node* new_neighbor = (struct node*)malloc(sizeof(struct node));
    if(new_neighbor == NULL){
        printf("Error: Memory allocation failed.\n");
        exit(1);
    }
    new_neighbor->id = neighbour_node->id;
    new_neighbor->next = NULL;
    if(current_node->next == NULL){
        current_node->next = new_neighbor;
    }
    else{
        struct node* temp = current_node->next;
        while(temp->next != NULL){
            temp = temp->next;
        }
        temp->next = new_neighbor;
    }
    current_node->neighbour_counter++;
}

struct node* find_node(long id){
    if(node_list[id] == NULL){
        return NULL;
    }
    else{
        return node_list[id];
    }
}

void read_file(char* input_filename){
    FILE* input_file = fopen(input_filename, "r");
    long id, neighbour_id;
    char* line = NULL;
    size_t line_capacity = 0;
    struct node* current_node;
    struct node* neighbour_node;
    if(input_file == NULL){
        printf("Error: Failed to open input file.\n");
        exit(1);
    }
    while(getline(&line, &line_capacity, input_file) != -1){
        if(line[0] == '#'){
            continue;
        }
        if(sscanf(line, "%ld %ld", &id, &neighbour_id) == 2){
            if(id > max_node_id){
                max_node_id = id;
            }
            if(neighbour_id > max_node_id){
                max_node_id = neighbour_id;
            }
            current_node = find_node(id);
            if(current_node == NULL){
                current_node = create_node(id);
            }
            neighbour_node = find_node(neighbour_id);
            if(neighbour_node == NULL){
                neighbour_node = create_node(neighbour_id);
            }
            add_neighbour(current_node, neighbour_node);
        }
        else{
            printf("Error: Invalid line format.\n");
        }
    }
    free(line);
    fclose(input_file);
}

void* calculate_pagerank(void* arg){
    int i;
    double* portions = (double*)malloc(MAX_NODES * sizeof(double));
    struct node* current;
    struct node* neighbour;
    struct thread_arguments* args = (struct thread_arguments*)arg;
    int start_node = args->start_node;
    int end_node = args->end_node;
    if(portions == NULL){
        printf("Error: Memory allocation failed.\n");
        exit(1);
    }
    for(i = start_node ; i <= end_node ; i++){
        current = node_list[i];
        if(current == NULL){
            continue;
        }
        if(current->neighbour_counter != 0){
            portions[i] = (0.85 * current->pagerank) / current->neighbour_counter;
        }
        else{
            portions[i] = 0;
        }
    }
    pthread_barrier_wait(&barrier);
    for(i = start_node ; i <= end_node ; i++){
        current = node_list[i];
        if(current == NULL){
            continue;
        }
        current->pagerank -= portions[i] * current->neighbour_counter;
        neighbour = current->next;
        while(neighbour != NULL){
            node_list[neighbour->id]->pagerank += portions[i];
            neighbour = neighbour->next;
        }
    }
    free(portions);
    free(args);
    return NULL;
}

void print_to_file(){
    FILE *output_file = fopen("pagerank.csv", "w");
    int id;
    if(output_file == NULL){
        printf("Error: Failed to open output file.\n");
        exit(1);
    }
    fprintf(output_file, "node, pagerank\n");
    for(id = 0 ; id < max_node_id + 1 ; id++){
        struct node* current = node_list[id];
        if(current != NULL){
            fprintf(output_file, "%ld, %.3f\n", current->id, current->pagerank);
        }
    }
}

void free_node_list(){
    int i;
    for(i = 0 ; i < max_node_id + 1 ; i++){
        struct node* current = node_list[i];
        while(current != NULL){
            struct node* temp = current;
            current = current->next;
            free(temp);
        }
    }
}

int main(int argc, char* argv[]){
    int i, iterations;
    int number_of_threads = 0;
    int nodes_per_thread, remainder_nodes, start_node;
    pthread_t id[4];
    if(argc != 3){
        printf("Usage: %s <input_file> <number_of_threads>\n", argv[0]);
        return 1;
    }
    if(atoi(argv[2]) < 1 || atoi(argv[2]) > 4){
        printf("Error: number_of_threads must be an integer value between 1 and 4.\n");
        return 1;
    }
    read_file(argv[1]);
    number_of_threads = atoi(argv[2]);
    nodes_per_thread = max_node_id / number_of_threads;
    remainder_nodes = max_node_id % number_of_threads;
    pthread_barrier_init(&barrier, NULL, number_of_threads);
    for(iterations = 0 ; iterations < 50 ; iterations++){
        start_node = 0;
        for(i = 0 ; i < number_of_threads ; i++){
            struct thread_arguments* args = (struct thread_arguments*)malloc(sizeof(struct thread_arguments));
            args->start_node = start_node;
            args->end_node = start_node + nodes_per_thread - 1;
            if(i == number_of_threads - 1){
                args->end_node += remainder_nodes + 1;
            }
            pthread_create(&id[i], NULL, calculate_pagerank, (void*)args);
            start_node = args->end_node + 1;
        }
        for(i = 0 ; i < number_of_threads ; i++){
            pthread_join(id[i], NULL);
        }
    }
    print_to_file();
    free_node_list();
    return 0;
}
