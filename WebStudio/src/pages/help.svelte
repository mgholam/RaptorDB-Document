<script>
  export let active = false;
</script>

<svelte:options accessors={true} />
<!-- TODO : add more help description 
-->
{#if active}
  <div class="tab-content" class:active>
    <h2>Welcome to the RaptorDB Web Studio</h2>
    You can use this Web interface to query and see configuration information.
    <!-- <br />
    <br />
    Server address = {window.ServerURL} -->
  </div>
{/if}
