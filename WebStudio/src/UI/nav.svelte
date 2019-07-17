<script>
  import { createEventDispatcher } from "svelte";

  const dispatch = createEventDispatcher();

  let activepanel = "";

  let navdata = {
    Query: [
      { name: "query", description: "Query" },
      { name: "schema", description: "View Schema" }
    ],
    Documents: [
      { name: "docview", description: "View" },
      { name: "dochistory", description: "History" },
      { name: "docsearch", description: "Full Text Search" }
    ],
    "High Frequency Store": [{ name: "hfbrowser", description: "Browse" }],
    System: [
      { name: "sysinfo", description: "Information" },
      { name: "sysconfig", description: "Configs" }
    ]
  };

  function togglepanel(name) {
    if (activepanel === name) activepanel = "";
    else activepanel = name;
  }
</script>

<style>
  button.accordion {
    background-color: #333333;
    color: #bbb;
    cursor: pointer;
    padding: 18px;
    width: 97%;
    border: none;
    text-align: left;
    outline: none;
    font-size: 15px;
    transition: 0.3s;
    margin-bottom: 5px;
  }

  button.accordion.active {
    background-color: #666666;
    color: #eee;
    margin-bottom: 0;
  }

  button.accordion:hover {
    background-color: #999999;
  }

  button.accordion:after {
    content: "\02795";
    font-size: 13px;
    color: #666666;
    float: right;
    margin-left: 5px;
  }

  button.accordion.active:after {
    content: "\2796";
  }

  div.panel {
    padding: 0 18px;
    background-color: white;
    max-height: 0;
    overflow: hidden;
    transition: 0.1s ease-out;
    opacity: 0;
  }

  div.panel.show {
    opacity: 1;
    max-height: 500px;
    min-height: 100px;
    padding: 5px;
    width: 203px;
  }

  div.panel.show label {
    cursor: pointer;
    display: block;
    padding: 5px;
  }

  div.panel.show label:hover {
    background-color: #999999;
  }
</style>

<div>
  <img src="./raptordb.png" alt="logo" />

  {#each Object.keys(navdata) as name}
    <button
      class="accordion"
      class:active={activepanel === name}
      on:click={() => togglepanel(name)}>
      {name}
    </button>
    <div class="panel" class:show={activepanel === name}>
      {#each navdata[name] as panel}
        <label 
          id={panel.name}
          on:click={() => {
            dispatch('navclick', panel);
          }}>
          {panel.description}
        </label>
      {/each}
    </div>
  {/each}
</div>
