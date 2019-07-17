<script>
  import { onMount } from "svelte";

  export let active = false;
  let viewname = "";
  let viewnames = [];

  let schema = {};

  let FullTextColumns = [];
  let CaseInsensitiveColumns = [];
  let StringIndexLength = [];
  let NoIndexingColumns = [];

  let Name = "";
  let Description = "";
  let Version = "";
  let isPrimaryList = "";
  let isActive = "";
  let DeleteBeforeInsert = "";
  let BackgroundIndexing = "";
  let ConsistentSaveToThisView = "";
  let TransactionMode = "";

  onMount(() => {
    // get views from server
    window.GET("RaptorDB/GetViews", function(data) {
      // console.log(data);
      var vn = [];
      data.Rows.forEach(x => vn.push(x.Name));
      viewnames = vn;
      if (vn.length > 0) {
        viewname = vn[0];
        schemachanged();
      }
    });
  });

  function schemachanged() {
    window.GET("RaptorDB/viewinfo?" + viewname, function(data) {
      schema = data.Schema;
      FullTextColumns = data.View["FullTextColumns"];
      CaseInsensitiveColumns = data.View["CaseInsensitiveColumns"];
      StringIndexLength = data.View["StringIndexLength"];
      NoIndexingColumns = data.View["NoIndexingColumns"];
      // console.log(data);
      Name = data.View["Name"];
      Description = data.View["Description"];
      Version = data.View["Version"];
      isPrimaryList = data.View["isPrimaryList"];
      isActive = data.View["isActive"];
      DeleteBeforeInsert = data.View["DeleteBeforeInsert"];
      BackgroundIndexing = data.View["BackgroundIndexing"];
      ConsistentSaveToThisView = data.View["ConsistentSaveToThisView"];
      TransactionMode = data.View["TransactionMode"];
    });
  }
</script>

<style>
  .bold {
    font-weight: 700;
  }
  .ListBox {
    border-style: solid;
    border-color: gray;
    background-color: white;
    padding: 5px;
    height: 500px;
    overflow: scroll;
    height: 150px;
  }

  .collist {
    display: block;
    font-weight: bold;
  }
  /* .schema_def table {
    border-style: solid;
  } */

  .schema_def th {
    background-color: #cccccc;
  }

  .schema_def td:nth-child(odd) {
    font-weight: bold;
    text-align: right;
  }

  .schema_def td:nth-child(even) {
    color: green;
  }
</style>

<svelte:options accessors={true} />

{#if active}
  <div class="tab-content" class:active>
    <label>View Name :</label>
    <select style="width:200px; font-size: larger" on:change={schemachanged} bind:value={viewname}>
      {#each viewnames as vn (vn)}
        <option>{vn}</option>
      {/each}
    </select>
    <br />
    <br />
    <div class="View">
      <label>Name :</label>
      <label class="bold">{Name}</label>
      <br />
      <label>Description :</label>
      <label class="bold">{Description}</label>
      <br />
      <label>Version :</label>
      <label class="bold">{Version}</label>
      <br />
      <label>is Primary List :</label>
      <label class="bold">{isPrimaryList}</label>
      <br />
      <label>is Active :</label>
      <label class="bold">{isActive}</label>
      <br />
      <label>Delete before insert :</label>
      <label class="bold">{DeleteBeforeInsert}</label>
      <br />
      <label>Background Indexing :</label>
      <label class="bold">{BackgroundIndexing}</label>
      <br />
      <label>Consistent Save to View :</label>
      <label class="bold">{ConsistentSaveToThisView}</label>
      <br />
      <label>Transaction Mode :</label>
      <label class="bold">{TransactionMode}</label>
      <br />
      <label>Schema Definition :</label>
      <table class="schema_def">
        <th>Column Name</th>
        <th>Type</th>
        {#each Object.keys(schema) as sc (sc)}
          <tr>
            <td>{sc}</td>
            <td>{schema[sc]}</td>
          </tr>
        {/each}
      </table>
      <table width="100%">
        <tr>
          <td>Full Text Columns :</td>
          <td>Case Insensitive Columns :</td>
          <td>String Index Length :</td>
          <td>No Indexing Columns :</td>
        </tr>
        <tr>
          <td width="25%">
            <div class="ListBox">
              {#each FullTextColumns as fc (fc)}
                <label class="collist">{fc}</label>
              {/each}
            </div>
          </td>
          <td width="25%">
            <div class="ListBox">
              {#each CaseInsensitiveColumns as fc (fc)}
                <label class="collist">{fc}</label>
              {/each}
            </div>
          </td>
          <td width="25%">
            <div class="ListBox">
              {#each StringIndexLength as fc (fc)}
                <label class="collist">{fc}</label>
              {/each}
            </div>
          </td>
          <td width="25%">
            <div class="ListBox">
              {#each NoIndexingColumns as fc (fc)}
                <label class="collist">{fc}</label>
              {/each}
            </div>
          </td>
        </tr>
      </table>
    </div>
  </div>
{/if}
